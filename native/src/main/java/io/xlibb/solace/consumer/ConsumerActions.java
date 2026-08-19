/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org).
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

package io.xlibb.solace.consumer;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.xlibb.solace.common.CommonUtils;
import io.xlibb.solace.config.ConfigurationUtils;
import io.xlibb.solace.config.ConsumerConfiguration;
import io.xlibb.solace.config.ConsumerSubscriptionConfig;
import io.xlibb.solace.config.QueueConsumerConfig;
import io.xlibb.solace.config.TopicConsumerConfig;
import io.xlibb.solace.observability.SolaceMetricsUtil;
import io.xlibb.solace.observability.SolaceSessionEventHandler;
import io.xlibb.solace.observability.SolaceTracingUtil;

import java.math.BigDecimal;

import static io.xlibb.solace.common.Constants.NATIVE_CLOSED;
import static io.xlibb.solace.common.Constants.NATIVE_CONSUMER;
import static io.xlibb.solace.common.Constants.NATIVE_DESTINATION;
import static io.xlibb.solace.common.Constants.NATIVE_DESTINATION_KIND;
import static io.xlibb.solace.common.Constants.NATIVE_EVENT_HANDLER;
import static io.xlibb.solace.common.Constants.NATIVE_FLOW;
import static io.xlibb.solace.common.Constants.NATIVE_SESSION;
import static io.xlibb.solace.common.Constants.NATIVE_SUBSCRIPTION_TYPE;
import static io.xlibb.solace.common.Constants.NATIVE_TRANSACTED;
import static io.xlibb.solace.common.Constants.NATIVE_TX_SESSION;
import static io.xlibb.solace.common.Constants.NATIVE_URL;
import static io.xlibb.solace.common.Constants.NATIVE_VPN;
import static io.xlibb.solace.consumer.ConsumerUtils.SUBSCRIPTION_TYPE_DIRECT_TOPIC;
import static io.xlibb.solace.consumer.ConsumerUtils.SUBSCRIPTION_TYPE_DURABLE_TOPIC;
import static io.xlibb.solace.consumer.ConsumerUtils.SUBSCRIPTION_TYPE_QUEUE;
import static io.xlibb.solace.consumer.ConsumerUtils.createDirectTopicConsumer;
import static io.xlibb.solace.consumer.ConsumerUtils.createDurableTopicConsumer;
import static io.xlibb.solace.consumer.ConsumerUtils.createQueueConsumer;
import static io.xlibb.solace.observability.SolaceMetricsUtil.reportConsumerFailure;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.CONTEXT_CONSUMER;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.DESTINATION_KIND_QUEUE;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.DESTINATION_KIND_TOPIC;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_ACKNOWLEDGE;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_CLOSE;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_COMMIT;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_NACK;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_RECEIVE;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_ROLLBACK;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.UNKNOWN;

/**
 * Consumer actions - main entry point for Ballerina MessageConsumer interop.
 */
public class ConsumerActions {

    /**
     * Initialize the consumer with connection URL and configuration. Creates either a transacted or non-transacted
     * consumer based on configuration.
     *
     * @param consumer the Ballerina consumer object
     * @param url      the broker URL
     * @param config   the consumer configuration
     * @return null on success, BError on failure
     */
    public static BError init(BObject consumer, BString url, BMap<BString, Object> config) {
        String vpnName = UNKNOWN;
        try {
            // Parse configuration
            ConsumerConfiguration consumerConfig = new ConsumerConfiguration(config);
            ConsumerSubscriptionConfig subscriptionConfig = consumerConfig.subscriptionConfig();
            boolean isTransacted = consumerConfig.connectionConfig().transacted();
            vpnName = consumerConfig.connectionConfig().vpnName();

            // Build JCSMP properties from configuration
            JCSMPProperties jcsmpProps =
                    ConfigurationUtils.buildJCSMPProperties(url.getValue(), consumerConfig.connectionConfig());

            // Create and connect base JCSMP session.
            SolaceSessionEventHandler eventHandler =
                    new SolaceSessionEventHandler(CONTEXT_CONSUMER, url.getValue(), vpnName);
            final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(jcsmpProps, null, eventHandler);
            session.connect();
            eventHandler.markConnected();

            // Validate: Direct topic subscriptions cannot be transacted
            if (isTransacted && subscriptionConfig instanceof TopicConsumerConfig topicConfig &&
                    !topicConfig.isDurable()) {
                return CommonUtils.createError("Transacted mode is not supported for direct topic subscriptions. " +
                        "Use DURABLE endpoint type for guaranteed delivery with transactions.");
            }

            // Create TransactedSession if in transacted mode
            final TransactedSession txSession = isTransacted ? session.createTransactedSession() : null;

            // Store session references and transacted flag
            consumer.addNativeData(NATIVE_SESSION, session);
            consumer.addNativeData(NATIVE_TX_SESSION, txSession);
            consumer.addNativeData(NATIVE_TRANSACTED, isTransacted);
            consumer.addNativeData(NATIVE_CLOSED, false);
            consumer.addNativeData(NATIVE_URL, url.getValue());
            consumer.addNativeData(NATIVE_VPN, vpnName);
            consumer.addNativeData(NATIVE_EVENT_HANDLER, eventHandler);

            // Store destination name and kind for observability
            String destinationName = ConsumerUtils.extractDestinationName(subscriptionConfig);
            consumer.addNativeData(NATIVE_DESTINATION, destinationName);
            consumer.addNativeData(NATIVE_DESTINATION_KIND,
                    subscriptionConfig instanceof QueueConsumerConfig ? DESTINATION_KIND_QUEUE
                            : DESTINATION_KIND_TOPIC);

            // Create appropriate consumer based on subscription type
            if (subscriptionConfig instanceof QueueConsumerConfig queueConfig) {
                FlowReceiverFactory factory = isTransacted
                        ? props -> txSession.createFlow(null, props, null)
                        : props -> session.createFlow(null, props);
                createQueueConsumer(consumer, factory, queueConfig, isTransacted);
            } else if (subscriptionConfig instanceof TopicConsumerConfig topicConfig) {
                topicConfig.validate();
                if (topicConfig.isDurable()) {
                    FlowReceiverFactory factory = isTransacted
                            ? props -> txSession.createFlow(null, props, null)
                            : props -> session.createFlow(null, props);
                    createDurableTopicConsumer(consumer, factory, topicConfig, isTransacted);
                } else {
                    createDirectTopicConsumer(consumer, session, topicConfig);
                }
            } else {
                return CommonUtils.createError("Unknown subscription configuration type");
            }

        } catch (Exception e) {
            SolaceMetricsUtil.reportConnectionError(CONTEXT_CONSUMER, url.getValue(), vpnName);
            return CommonUtils.createError("Failed to initialize consumer", e);
        }

        // Observability only, deliberately outside the block above: the consumer is fully created by this point, so a
        // failure here must not report an init failure for an init that succeeded.
        SolaceMetricsUtil.reportNewConsumer(consumer);
        return null;
    }

    /**
     * Receive a message with timeout.
     *
     * @param env      the Ballerina environment (injected for tracing)
     * @param consumer the Ballerina consumer object
     * @param timeout  the timeout in seconds
     * @return the received message, null if timeout, or BError on failure
     */
    public static Object receive(Environment env, BObject consumer, BDecimal timeout) {
        SolaceTracingUtil.traceResourceInvocation(env, consumer);
        Boolean closed = (Boolean) consumer.getNativeData(NATIVE_CLOSED);
        if (closed != null && closed) {
            return reportConsumerFailure(consumer, ERROR_TYPE_RECEIVE, "Consumer is closed");
        }
        long timeoutMs = timeout.decimalValue().multiply(BigDecimal.valueOf(1000)).longValue();
        String subscriptionType = (String) consumer.getNativeData(NATIVE_SUBSCRIPTION_TYPE);

        try {
            Object result = CommonUtils.executeBlocking(() -> {
                BytesXMLMessage message = null;
                if (SUBSCRIPTION_TYPE_QUEUE.equals(subscriptionType) ||
                        SUBSCRIPTION_TYPE_DURABLE_TOPIC.equals(subscriptionType)) {
                    FlowReceiver flowReceiver = (FlowReceiver) consumer.getNativeData(NATIVE_FLOW);
                    if (flowReceiver == null) {
                        return CommonUtils.createError("Consumer flow not initialized");
                    }
                    message = flowReceiver.receive((int) timeoutMs);
                } else if (SUBSCRIPTION_TYPE_DIRECT_TOPIC.equals(subscriptionType)) {
                    XMLMessageConsumer xmlConsumer = (XMLMessageConsumer) consumer.getNativeData(NATIVE_CONSUMER);
                    if (xmlConsumer == null) {
                        return CommonUtils.createError("Consumer not initialized");
                    }
                    message = xmlConsumer.receive((int) timeoutMs);
                }
                if (message == null) {
                    return null; // Timeout - no message available
                }
                try {
                    return MessageConverter.toBallerinaMessage(message);
                } catch (Exception e) {
                    return CommonUtils.createError("Failed to receive message", e);
                }
            });

            if (result instanceof BError bError) {
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_RECEIVE);
                return bError;
            }
            reportReceiveOutcome(consumer, result);
            return result;
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_RECEIVE);
            return CommonUtils.createError("Failed to receive message", e);
        }
    }

    /**
     * Receive a message without waiting.
     *
     * @param env      the Ballerina environment (injected for tracing)
     * @param consumer the Ballerina consumer object
     * @return the received message, null if none available, or BError on failure
     */
    public static Object receiveNoWait(Environment env, BObject consumer) {
        SolaceTracingUtil.traceResourceInvocation(env, consumer);
        Boolean closed = (Boolean) consumer.getNativeData(NATIVE_CLOSED);
        if (closed != null && closed) {
            return reportConsumerFailure(consumer, ERROR_TYPE_RECEIVE, "Consumer is closed");
        }
        String subscriptionType = (String) consumer.getNativeData(NATIVE_SUBSCRIPTION_TYPE);
        try {
            Object result = CommonUtils.executeBlocking(() -> {
                BytesXMLMessage message = null;
                if (SUBSCRIPTION_TYPE_QUEUE.equals(subscriptionType) ||
                        SUBSCRIPTION_TYPE_DURABLE_TOPIC.equals(subscriptionType)) {
                    FlowReceiver flowReceiver = (FlowReceiver) consumer.getNativeData(NATIVE_FLOW);
                    if (flowReceiver == null) {
                        return CommonUtils.createError("Consumer flow not initialized");
                    }
                    message = flowReceiver.receiveNoWait();
                } else if (SUBSCRIPTION_TYPE_DIRECT_TOPIC.equals(subscriptionType)) {
                    XMLMessageConsumer xmlConsumer = (XMLMessageConsumer) consumer.getNativeData(NATIVE_CONSUMER);
                    if (xmlConsumer == null) {
                        return CommonUtils.createError("Consumer not initialized");
                    }
                    message = xmlConsumer.receiveNoWait();
                }
                if (message == null) {
                    return null;
                }
                try {
                    return MessageConverter.toBallerinaMessage(message);
                } catch (Exception e) {
                    return CommonUtils.createError("Failed to receive message", e);
                }
            });

            if (result instanceof BError bError) {
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_RECEIVE);
                return bError;
            }
            reportReceiveOutcome(consumer, result);
            return result;
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_RECEIVE);
            return CommonUtils.createError("Failed to receive message", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void reportReceiveOutcome(BObject consumer, Object result) {
        if (result == null) {
            SolaceMetricsUtil.reportEmptyReceive(consumer);
            return;
        }
        BMap<BString, Object> receivedMessage = (BMap<BString, Object>) result;
        SolaceMetricsUtil.reportConsume(consumer, CommonUtils.getPayloadSize(receivedMessage),
                CommonUtils.isRedelivered(receivedMessage));
    }

    /**
     * Acknowledge a message.
     *
     * @param consumer the Ballerina consumer object
     * @param message  the Ballerina message to acknowledge
     * @return null on success, BError on failure
     */
    public static BError acknowledge(BObject consumer, BMap<BString, Object> message) {
        try {
            Boolean closed = (Boolean) consumer.getNativeData(NATIVE_CLOSED);
            if (closed != null && closed) {
                return reportConsumerFailure(consumer, ERROR_TYPE_ACKNOWLEDGE, "Consumer is closed");
            }

            XMLMessage nativeMessage = MessageConverter.extractNativeMessage(message);
            if (nativeMessage == null) {
                return reportConsumerFailure(consumer, ERROR_TYPE_ACKNOWLEDGE,
                        "Cannot acknowledge: native message not found");
            }

            Object result = CommonUtils.executeBlocking(nativeMessage::ackMessage);
            if (result instanceof BError) {
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_ACKNOWLEDGE);
                return (BError) result;
            }
            SolaceMetricsUtil.reportAck(consumer);
            return null;
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_ACKNOWLEDGE);
            return CommonUtils.createError("Failed to acknowledge message", e);
        }
    }

    /**
     * Negatively acknowledge a message (NACK).
     *
     * @param consumer the Ballerina consumer object
     * @param message  the Ballerina message to NACK
     * @param requeue  if true, use FAILED outcome (requeue); if false, use REJECTED outcome (DMQ)
     * @return null on success, BError on failure
     */
    public static BError nack(BObject consumer, BMap<BString, Object> message, boolean requeue) {
        try {
            Boolean closed = (Boolean) consumer.getNativeData(NATIVE_CLOSED);
            if (closed != null && closed) {
                return reportConsumerFailure(consumer, ERROR_TYPE_NACK, "Consumer is closed");
            }

            XMLMessage nativeMessage = MessageConverter.extractNativeMessage(message);
            if (nativeMessage == null) {
                return reportConsumerFailure(consumer, ERROR_TYPE_NACK, "Cannot NACK: native message not found");
            }

            // Use settle() with appropriate outcome
            Object result = CommonUtils.executeBlocking(() -> {
                XMLMessage.Outcome outcome = requeue ? XMLMessage.Outcome.FAILED : XMLMessage.Outcome.REJECTED;
                nativeMessage.settle(outcome);
                return null;
            });
            if (result instanceof BError) {
                // As with acknowledge(), a broker-side settle failure arrives here as a BError, not as an exception.
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_NACK);
                return (BError) result;
            }
            SolaceMetricsUtil.reportNack(consumer, requeue);
            return null;
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_NACK);
            return CommonUtils.createError("Failed to NACK message", e);
        }
    }

    /**
     * Commit the current transaction. Only valid for transacted consumers (when connectionConfig.transacted = true).
     *
     * @param consumer the Ballerina consumer object
     * @return null on success, BError on failure
     */
    public static BError commit(BObject consumer) {
        try {
            Boolean closed = (Boolean) consumer.getNativeData(NATIVE_CLOSED);
            if (closed != null && closed) {
                return reportConsumerFailure(consumer, ERROR_TYPE_COMMIT, "Consumer is closed");
            }

            Boolean transacted = (Boolean) consumer.getNativeData(NATIVE_TRANSACTED);
            if (transacted == null || !transacted) {
                return reportConsumerFailure(consumer, ERROR_TYPE_COMMIT,
                        "commit() can only be called on transacted consumers. " +
                                "Set connectionConfig.transacted = true to enable transactions.");
            }

            TransactedSession txSession = (TransactedSession) consumer.getNativeData(NATIVE_TX_SESSION);
            if (txSession == null) {
                return reportConsumerFailure(consumer, ERROR_TYPE_COMMIT, "TransactedSession not initialized");
            }

            // Commit transaction on TransactedSession (blocking operation)
            Object result = CommonUtils.executeBlocking(txSession::commit);

            if (result instanceof BError) {
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_COMMIT);
                return (BError) result;
            }

            return null;
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_COMMIT);
            return CommonUtils.createError("Failed to commit transaction", e);
        }
    }

    /**
     * Rollback the current transaction. Only valid for transacted consumers (when connectionConfig.transacted = true).
     *
     * @param consumer the Ballerina consumer object
     * @return null on success, BError on failure
     */
    public static BError rollback(BObject consumer) {
        try {
            Boolean closed = (Boolean) consumer.getNativeData(NATIVE_CLOSED);
            if (closed != null && closed) {
                return reportConsumerFailure(consumer, ERROR_TYPE_ROLLBACK, "Consumer is closed");
            }

            Boolean transacted = (Boolean) consumer.getNativeData(NATIVE_TRANSACTED);
            if (transacted == null || !transacted) {
                return reportConsumerFailure(consumer, ERROR_TYPE_ROLLBACK,
                        "rollback() can only be called on transacted consumers. " +
                                "Set connectionConfig.transacted = true to enable transactions.");
            }

            TransactedSession txSession = (TransactedSession) consumer.getNativeData(NATIVE_TX_SESSION);
            if (txSession == null) {
                return reportConsumerFailure(consumer, ERROR_TYPE_ROLLBACK, "TransactedSession not initialized");
            }

            // Rollback transaction on TransactedSession (blocking operation)
            Object result = CommonUtils.executeBlocking(txSession::rollback);

            if (result instanceof BError) {
                SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_ROLLBACK);
                return (BError) result;
            }

            return null;
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_ROLLBACK);
            return CommonUtils.createError("Failed to rollback transaction", e);
        }
    }

    /**
     * Close the consumer and release resources. Closes flow receiver/consumer, transacted session (if any), and base
     * session in that order.
     *
     * @param env      the Ballerina environment (injected for tracing)
     * @param consumer the Ballerina consumer object
     * @return null on success, BError on failure
     */
    public static BError close(Environment env, BObject consumer) {
        SolaceTracingUtil.traceResourceInvocation(env, consumer);
        try {
            String subscriptionType = (String) consumer.getNativeData(NATIVE_SUBSCRIPTION_TYPE);

            // Close flow receiver or XML consumer
            if (SUBSCRIPTION_TYPE_QUEUE.equals(subscriptionType) ||
                    SUBSCRIPTION_TYPE_DURABLE_TOPIC.equals(subscriptionType)) {
                FlowReceiver flowReceiver = (FlowReceiver) consumer.getNativeData(NATIVE_FLOW);
                if (flowReceiver != null) {
                    flowReceiver.stop();
                    flowReceiver.close();
                }
            } else if (SUBSCRIPTION_TYPE_DIRECT_TOPIC.equals(subscriptionType)) {
                XMLMessageConsumer xmlConsumer = (XMLMessageConsumer) consumer.getNativeData(NATIVE_CONSUMER);
                if (xmlConsumer != null) {
                    xmlConsumer.stop();
                    xmlConsumer.close();
                }
            }

            // Close transacted session if present
            TransactedSession txSession = (TransactedSession) consumer.getNativeData(NATIVE_TX_SESSION);
            if (txSession != null) {
                txSession.close();
            }

            // Close base session
            JCSMPSession session = (JCSMPSession) consumer.getNativeData(NATIVE_SESSION);
            if (session != null) {
                session.closeSession();
            }

            // Mark as closed and clear native data
            consumer.addNativeData(NATIVE_CLOSED, true);
            consumer.addNativeData(NATIVE_FLOW, null);
            consumer.addNativeData(NATIVE_CONSUMER, null);
            consumer.addNativeData(NATIVE_TX_SESSION, null);
            consumer.addNativeData(NATIVE_TRANSACTED, null);
            consumer.addNativeData(NATIVE_SESSION, null);

            SolaceMetricsUtil.reportConsumerClose(consumer);
            return null;
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(consumer, ERROR_TYPE_CLOSE);
            return CommonUtils.createError("Failed to close consumer", e);
        } finally {
            SolaceSessionEventHandler.markDisconnected(consumer);
        }
    }
}
