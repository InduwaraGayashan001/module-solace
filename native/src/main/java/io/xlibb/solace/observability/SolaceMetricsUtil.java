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

package io.xlibb.solace.observability;

import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.observability.ObserveUtils;
import io.ballerina.runtime.observability.metrics.DefaultMetricRegistry;
import io.ballerina.runtime.observability.metrics.MetricId;
import io.ballerina.runtime.observability.metrics.MetricRegistry;
import io.ballerina.runtime.observability.metrics.StatisticConfig;
import io.xlibb.solace.common.CommonUtils;

import static io.xlibb.solace.common.Constants.NATIVE_DESTINATION;
import static io.xlibb.solace.common.Constants.NATIVE_DESTINATION_KIND;
import static io.xlibb.solace.common.Constants.NATIVE_URL;
import static io.xlibb.solace.common.Constants.NATIVE_VPN;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.CONFIRM_ACCEPTED;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.CONFIRM_REJECTED;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.CONNECTOR_NAME;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.CONTEXT_CONSUMER;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.CONTEXT_PRODUCER;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_CONNECTION;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_ACKS;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_CONNECTIONS_UP;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_CONSUMED;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_CONSUMED_SIZE;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_CONSUMERS;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_EMPTY_RECEIVES;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_ERRORS;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_NACKS;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_PROCESS_DURATION;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_PUBLISHED;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_PUBLISHED_SIZE;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_PUBLISHERS;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_PUBLISH_CONFIRMS;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_PUBLISH_DURATION;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_RECONNECTS;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.METRIC_REDELIVERED;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.NACK_OUTCOME_DMQ;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.NACK_OUTCOME_REQUEUE;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.TAG_KEY_ERROR_TYPE;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.TAG_KEY_EVENT;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.TAG_KEY_OUTCOME;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.TAG_KEY_RESULT;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.UNKNOWN;

/**
 * Metrics utility for the Solace connector.
 */
public class SolaceMetricsUtil {

    private static final MetricRegistry metricRegistry = DefaultMetricRegistry.getInstance();

    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    /**
     * Percentiles reported for the connector's duration metrics.
     */
    private static final StatisticConfig DURATION_STATS = StatisticConfig.builder()
            .percentiles(0.5, 0.95, 0.99)
            .build();

    public static void reportNewProducer(BObject producer) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        incrementGauge(producerContext(producer), METRIC_PUBLISHERS[0], METRIC_PUBLISHERS[1]);
    }

    public static void reportNewConsumer(BObject consumer) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        incrementGauge(consumerContext(consumer), METRIC_CONSUMERS[0], METRIC_CONSUMERS[1]);
    }

    public static void reportProducerClose(BObject producer) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        decrementGauge(producerContext(producer), METRIC_PUBLISHERS[0], METRIC_PUBLISHERS[1]);
    }

    public static void reportConsumerClose(BObject consumer) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        decrementGauge(consumerContext(consumer), METRIC_CONSUMERS[0], METRIC_CONSUMERS[1]);
    }

    /**
     * Reports a successful publish along with how long the publish call took.
     */
    public static void reportPublish(BObject producer, String destination, String destinationKind,
                                     String deliveryMode, int size, long durationNanos) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        SolaceObserverContext ctx = publishContext(producer, destination, destinationKind, deliveryMode);
        incrementCounter(ctx, METRIC_PUBLISHED[0], METRIC_PUBLISHED[1], 1);
        incrementCounter(ctx, METRIC_PUBLISHED_SIZE[0], METRIC_PUBLISHED_SIZE[1], size);
        recordDuration(ctx, METRIC_PUBLISH_DURATION[0], METRIC_PUBLISH_DURATION[1], durationNanos);
    }

    /**
     * Reports a broker acknowledgement (or rejection) of a previously published guaranteed message.
     */
    public static void reportPublishConfirm(String url, String vpn, boolean accepted) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        SolaceObserverContext ctx = new SolaceObserverContext(CONTEXT_PRODUCER, url).withVpn(vpn)
                .withTag(TAG_KEY_RESULT, accepted ? CONFIRM_ACCEPTED : CONFIRM_REJECTED);
        incrementCounter(ctx, METRIC_PUBLISH_CONFIRMS[0], METRIC_PUBLISH_CONFIRMS[1], 1);
    }

    public static void reportConsume(BObject consumer, int size, boolean redelivered) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        reportConsume(getUrl(consumer), getVpn(consumer), getDestination(consumer),
                getDestinationKind(consumer), size, redelivered);
    }

    /**
     * Reports a consumed message for the push-based listener path.
     */
    public static void reportConsume(String url, String vpn, String destination, String destinationKind, int size,
                                     boolean redelivered) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        SolaceObserverContext ctx = consumeContext(url, vpn, destination, destinationKind);
        incrementCounter(ctx, METRIC_CONSUMED[0], METRIC_CONSUMED[1], 1);
        incrementCounter(ctx, METRIC_CONSUMED_SIZE[0], METRIC_CONSUMED_SIZE[1], size);
        if (redelivered) {
            incrementCounter(ctx, METRIC_REDELIVERED[0], METRIC_REDELIVERED[1], 1);
        }
    }

    /**
     * Reports a call that completed without a message (the poll timed out). 
     */
    public static void reportEmptyReceive(BObject consumer) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        incrementCounter(consumerDestinationContext(consumer), METRIC_EMPTY_RECEIVES[0], METRIC_EMPTY_RECEIVES[1], 1);
    }

    public static void reportAck(BObject consumer) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        incrementCounter(consumerDestinationContext(consumer), METRIC_ACKS[0], METRIC_ACKS[1], 1);
    }

    /**
     * Reports a negative acknowledgement.
     */
    public static void reportNack(BObject consumer, boolean requeue) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        SolaceObserverContext ctx = consumerDestinationContext(consumer)
                .withTag(TAG_KEY_OUTCOME, requeue ? NACK_OUTCOME_REQUEUE : NACK_OUTCOME_DMQ);
        incrementCounter(ctx, METRIC_NACKS[0], METRIC_NACKS[1], 1);
    }

    /**
     * Records how long a push-based service's {@code onMessage} took.
     */
    public static void reportProcessDuration(String url, String vpn, String destination, String destinationKind,
                                             long durationNanos) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        recordDuration(consumeContext(url, vpn, destination, destinationKind),
                METRIC_PROCESS_DURATION[0], METRIC_PROCESS_DURATION[1], durationNanos);
    }

    public static void reportProducerError(BObject producer, String errorType) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        SolaceObserverContext ctx = producerContext(producer);
        ctx.addTag(TAG_KEY_ERROR_TYPE, errorType);
        incrementCounter(ctx, METRIC_ERRORS[0], METRIC_ERRORS[1], 1);
    }

    public static void reportProducerError(BObject producer, String destination, String destinationKind,
                                           String errorType) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        SolaceObserverContext ctx = new SolaceObserverContext(CONTEXT_PRODUCER, getUrl(producer), destination)
                .withVpn(getVpn(producer))
                .withDestinationKind(destinationKind);
        ctx.addTag(TAG_KEY_ERROR_TYPE, errorType);
        incrementCounter(ctx, METRIC_ERRORS[0], METRIC_ERRORS[1], 1);
    }

    public static void reportConsumerError(BObject consumer, String errorType) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        SolaceObserverContext ctx = consumerDestinationContext(consumer);
        ctx.addTag(TAG_KEY_ERROR_TYPE, errorType);
        incrementCounter(ctx, METRIC_ERRORS[0], METRIC_ERRORS[1], 1);
    }

    /**
     * Counts a consumer-side failure and returns the error to hand back to the caller.
     */
    public static BError reportConsumerFailure(BObject consumer, String errorType, String errorMessage) {
        reportConsumerError(consumer, errorType);
        return CommonUtils.createError(errorMessage);
    }

    /**
     * Reports a consumer error for the push-based listener path.
     */
    public static void reportConsumerError(String url, String vpn, String destination, String destinationKind,
                                           String errorType) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        SolaceObserverContext ctx = consumeContext(url, vpn, destination, destinationKind);
        ctx.addTag(TAG_KEY_ERROR_TYPE, errorType);
        incrementCounter(ctx, METRIC_ERRORS[0], METRIC_ERRORS[1], 1);
    }

    /**
     * Reports a failure to establish a session.
     */
    public static void reportConnectionError(String context, String url, String vpn) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        SolaceObserverContext ctx = new SolaceObserverContext(context, url).withVpn(vpn);
        ctx.addTag(TAG_KEY_ERROR_TYPE, ERROR_TYPE_CONNECTION);
        incrementCounter(ctx, METRIC_ERRORS[0], METRIC_ERRORS[1], 1);
    }

    /**
     * Counts a session connectivity change signalled by JCSMP.
     */
    public static void reportSessionEvent(String context, String url, String vpn, String event) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        SolaceObserverContext ctx = new SolaceObserverContext(context, url).withVpn(vpn)
                .withTag(TAG_KEY_EVENT, event);
        incrementCounter(ctx, METRIC_RECONNECTS[0], METRIC_RECONNECTS[1], 1);
    }

    /**
     * Moves the count of currently connected sessions.
     * 
     * @param delta {@code +1} when a session becomes usable, {@code -1} when it stops being usable
     */
    static void adjustConnectionsUp(String context, String url, String vpn, int delta) {
        if (!ObserveUtils.isMetricsEnabled() || metricRegistry == null) {
            return;
        }
        SolaceObserverContext ctx = new SolaceObserverContext(context, url).withVpn(vpn);
        MetricId id = new MetricId(CONNECTOR_NAME + "_" + METRIC_CONNECTIONS_UP[0], METRIC_CONNECTIONS_UP[1],
                ctx.getAllTags());
        if (delta >= 0) {
            metricRegistry.gauge(id).increment(delta);
        } else {
            metricRegistry.gauge(id).decrement(-delta);
        }
    }

    private static SolaceObserverContext producerContext(BObject producer) {
        return new SolaceObserverContext(CONTEXT_PRODUCER, getUrl(producer)).withVpn(getVpn(producer));
    }

    private static SolaceObserverContext consumerContext(BObject consumer) {
        return new SolaceObserverContext(CONTEXT_CONSUMER, getUrl(consumer)).withVpn(getVpn(consumer));
    }

    private static SolaceObserverContext consumerDestinationContext(BObject consumer) {
        return consumeContext(getUrl(consumer), getVpn(consumer), getDestination(consumer),
                getDestinationKind(consumer));
    }

    private static SolaceObserverContext consumeContext(String url, String vpn, String destination,
                                                        String destinationKind) {
        return new SolaceObserverContext(CONTEXT_CONSUMER, url, destination)
                .withVpn(vpn)
                .withDestinationKind(destinationKind);
    }

    private static SolaceObserverContext publishContext(BObject producer, String destination, String destinationKind,
                                                        String deliveryMode) {
        return new SolaceObserverContext(CONTEXT_PRODUCER, getUrl(producer), destination)
                .withVpn(getVpn(producer))
                .withDestinationKind(destinationKind)
                .withDeliveryMode(deliveryMode);
    }

    static String getUrl(BObject object) {
        Object url = object.getNativeData(NATIVE_URL);
        return url instanceof String ? (String) url : UNKNOWN;
    }

    static String getDestination(BObject object) {
        Object dest = object.getNativeData(NATIVE_DESTINATION);
        return dest instanceof String ? (String) dest : UNKNOWN;
    }

    static String getVpn(BObject object) {
        Object vpn = object.getNativeData(NATIVE_VPN);
        return vpn instanceof String ? (String) vpn : UNKNOWN;
    }

    static String getDestinationKind(BObject object) {
        Object kind = object.getNativeData(NATIVE_DESTINATION_KIND);
        return kind instanceof String ? (String) kind : UNKNOWN;
    }

    private static void incrementCounter(SolaceObserverContext ctx, String name, String desc, int amount) {
        if (metricRegistry == null) {
            return;
        }
        metricRegistry.counter(new MetricId(CONNECTOR_NAME + "_" + name, desc, ctx.getAllTags()))
                .increment(amount);
    }

    private static void incrementGauge(SolaceObserverContext ctx, String name, String desc) {
        if (metricRegistry == null) {
            return;
        }
        metricRegistry.gauge(new MetricId(CONNECTOR_NAME + "_" + name, desc, ctx.getAllTags())).increment();
    }

    private static void decrementGauge(SolaceObserverContext ctx, String name, String desc) {
        if (metricRegistry == null) {
            return;
        }
        metricRegistry.gauge(new MetricId(CONNECTOR_NAME + "_" + name, desc, ctx.getAllTags())).decrement();
    }

    private static void recordDuration(SolaceObserverContext ctx, String name, String desc, long durationNanos) {
        if (metricRegistry == null || durationNanos < 0) {
            return;
        }
        metricRegistry.gauge(new MetricId(CONNECTOR_NAME + "_" + name, desc, ctx.getAllTags()), DURATION_STATS)
                .setValue(durationNanos / NANOS_PER_SECOND);
    }

    private SolaceMetricsUtil() {
    }
}
