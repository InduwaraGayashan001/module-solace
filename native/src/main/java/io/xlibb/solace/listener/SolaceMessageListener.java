/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.xlibb.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.XMLMessageListener;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.observability.ObservabilityConstants;
import io.ballerina.runtime.observability.ObserveUtils;
import io.xlibb.solace.common.CommonUtils;
import io.xlibb.solace.consumer.MessageConverter;
import io.xlibb.solace.observability.SolaceMetricsUtil;
import io.xlibb.solace.observability.SolaceObserverContext;
import io.xlibb.solace.observability.SolaceTracingUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static io.xlibb.solace.observability.SolaceObservabilityConstants.CONTEXT_CONSUMER;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_ACKNOWLEDGE;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_DISPATCH;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_RECEIVE;

/**
 * JCSMP asynchronous message listener that bridges broker delivery to a Ballerina service.
 * <p>
 * The broker pushes each message to {@link #onReceive(BytesXMLMessage)} on a JCSMP consumer-dispatch thread. To avoid
 * blocking that thread - which JCSMP also uses to process control responses such as transacted commits and to drive
 * redelivery - the message is converted to a Ballerina record on the delivery thread (so its payload is copied before
 * the buffer can be reused) and the service invocation plus any settlement (ack / nack / commit / rollback) are handed
 * off to a dedicated single-threaded executor. The single thread preserves per-flow message ordering while keeping the
 * delivery thread free.
 */
final class SolaceMessageListener implements XMLMessageListener {

    private static final String ON_MESSAGE = "onMessage";
    private static final String ON_ERROR = "onError";

    private final Runtime runtime;
    private final BObject service;
    private final BObject caller;
    private final boolean hasCaller;
    private final boolean hasOnError;
    private final boolean autoAck;
    private final String url;
    private final String vpn;
    private final String destination;
    private final String destinationKind;
    private final ExecutorService dispatcher;

    SolaceMessageListener(Runtime runtime, BObject service, BObject caller, boolean hasCaller, boolean hasOnError,
                          boolean autoAck, String url, String vpn, String destination, String destinationKind) {
        this.runtime = runtime;
        this.service = service;
        this.caller = caller;
        this.hasCaller = hasCaller;
        this.hasOnError = hasOnError;
        this.autoAck = autoAck;
        this.url = url;
        this.vpn = vpn;
        this.destination = destination;
        this.destinationKind = destinationKind;
        this.dispatcher = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "solace-listener-dispatch");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void onReceive(BytesXMLMessage message) {
        // Convert on the JCSMP delivery thread (copies the payload, safe for direct messages), then hand off so the
        // delivery thread is never blocked by the service call or a blocking settlement.
        BMap<BString, Object> ballerinaMessage;
        try {
            ballerinaMessage = MessageConverter.toBallerinaMessage(message);
        } catch (Throwable t) {
            // The broker delivered a message the connector could not decode - a receive-side failure.
            submit(() -> dispatchError(CommonUtils.createError("Failed to convert message",
                    t instanceof Exception e ? e : new Exception(t)), null, ERROR_TYPE_RECEIVE));
            return;
        }
        SolaceMetricsUtil.reportConsume(url, vpn, destination, destinationKind,
                CommonUtils.getPayloadSize(ballerinaMessage), CommonUtils.isRedelivered(ballerinaMessage));
        Map<String, String> traceContext = SolaceTracingUtil.extractTraceContextHeaders(ballerinaMessage);
        submit(() -> deliver(message, ballerinaMessage, traceContext));
    }

    private void deliver(BytesXMLMessage message, BMap<BString, Object> ballerinaMessage,
                         Map<String, String> traceContext) {
        long startNanos = System.nanoTime();
        try {
            Object result = invokeOnMessage(ballerinaMessage, traceContext);
            if (result instanceof BError bError) {
                dispatchError(bError, traceContext, ERROR_TYPE_DISPATCH);
                return;
            }
            // In AUTO_ACK mode the flow is created with client acknowledgement, so settle on success here.
            if (autoAck) {
                settleAutoAck(message, traceContext);
            }
        } catch (BError bError) {
            dispatchError(bError, traceContext, ERROR_TYPE_DISPATCH);
        } catch (Throwable t) {
            dispatchError(CommonUtils.createError("Failed to dispatch message to service",
                    t instanceof Exception e ? e : new Exception(t)), traceContext, ERROR_TYPE_DISPATCH);
        } finally {
            SolaceMetricsUtil.reportProcessDuration(url, vpn, destination, destinationKind,
                    System.nanoTime() - startNanos);
        }
    }

    /**
     * Settles a successfully processed message in AUTO_ACK mode.
     */
    private void settleAutoAck(BytesXMLMessage message, Map<String, String> traceContext) {
        try {
            message.ackMessage();
            SolaceMetricsUtil.reportAck(caller);
        } catch (Throwable t) {
            dispatchError(CommonUtils.createError("Failed to acknowledge message",
                    t instanceof Exception e ? e : new Exception(t)), traceContext, ERROR_TYPE_ACKNOWLEDGE);
        }
    }

    @Override
    public void onException(JCSMPException exception) {
        // A flow-level failure raised by JCSMP itself - a genuine receive-side error.
        submit(() -> dispatchError(CommonUtils.createError("Solace consumer flow error", exception),
                null, ERROR_TYPE_RECEIVE));
    }

    private Object invokeOnMessage(BMap<BString, Object> ballerinaMessage, Map<String, String> traceContext) {
        StrandMetadata metadata = new StrandMetadata(false, tracingProperties(traceContext));
        if (hasCaller) {
            return runtime.callMethod(service, ON_MESSAGE, metadata, ballerinaMessage, caller);
        }
        return runtime.callMethod(service, ON_MESSAGE, metadata, ballerinaMessage);
    }

    private void dispatchError(BError error, Map<String, String> traceContext, String errorType) {
        SolaceMetricsUtil.reportConsumerError(url, vpn, destination, destinationKind, errorType);
        if (!hasOnError) {
            return;
        }
        try {
            StrandMetadata metadata = new StrandMetadata(false, tracingProperties(traceContext));
            runtime.callMethod(service, ON_ERROR, metadata, error);
        } catch (Throwable ignored) {
            // Suppress secondary failures from the error handler to avoid losing the original error.
        }
    }

    private Map<String, Object> tracingProperties(Map<String, String> traceContext) {
        if (!ObserveUtils.isTracingEnabled()) {
            return null;
        }
        SolaceObserverContext ctx = new SolaceObserverContext(CONTEXT_CONSUMER, url, destination)
                .withVpn(vpn)
                .withDestinationKind(destinationKind);
        if (traceContext != null && !traceContext.isEmpty()) {
            ctx.addProperty(ObservabilityConstants.PROPERTY_TRACE_PROPERTIES, traceContext);
        }
        Map<String, Object> properties = new HashMap<>();
        properties.put(ObservabilityConstants.KEY_OBSERVER_CONTEXT, ctx);
        return properties;
    }

    private void submit(Runnable task) {
        try {
            dispatcher.execute(task);
        } catch (RejectedExecutionException ignored) {
            // The listener is stopping; drop late deliveries (unsettled guaranteed messages are redelivered).
        }
    }

    /**
     * Stops the dispatch executor. Called when the service is detached or the listener is stopped.
     */
    void shutdown() {
        dispatcher.shutdown();
        try {
            if (!dispatcher.awaitTermination(5, TimeUnit.SECONDS)) {
                dispatcher.shutdownNow();
            }
        } catch (InterruptedException e) {
            dispatcher.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
