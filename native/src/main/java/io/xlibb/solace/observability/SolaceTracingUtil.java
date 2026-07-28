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

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.observability.ObserveUtils;
import io.ballerina.runtime.observability.ObserverContext;
import io.ballerina.runtime.observability.tracer.TracersStore;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.xlibb.solace.common.MessageFieldConstants.PROPERTIES_KEY;
import static io.xlibb.solace.observability.SolaceMetricsUtil.getDestination;
import static io.xlibb.solace.observability.SolaceMetricsUtil.getUrl;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.TAG_KEY_DESTINATION;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.TAG_KEY_URL;

/**
 * Tracing utility for the Solace connector.
 */
public class SolaceTracingUtil {

    public static void traceResourceInvocation(Environment env, BObject object, String destination) {
        if (!ObserveUtils.isTracingEnabled()) {
            return;
        }
        ObserverContext ctx = ObserveUtils.getObserverContextOfCurrentFrame(env);
        if (ctx == null) {
            ctx = new ObserverContext();
            ObserveUtils.setObserverContextToCurrentFrame(env, ctx);
        }
        ctx.addTag(TAG_KEY_URL, getUrl(object));
        ctx.addTag(TAG_KEY_DESTINATION, destination);
    }

    public static void traceResourceInvocation(Environment env, BObject object) {
        if (!ObserveUtils.isTracingEnabled()) {
            return;
        }
        ObserverContext ctx = ObserveUtils.getObserverContextOfCurrentFrame(env);
        if (ctx == null) {
            ctx = new ObserverContext();
            ObserveUtils.setObserverContextToCurrentFrame(env, ctx);
        }
        ctx.addTag(TAG_KEY_URL, getUrl(object));
        ctx.addTag(TAG_KEY_DESTINATION, getDestination(object));
    }

    /**
     * Returns the current span's context serialized by the configured OpenTelemetry propagator.
     *
     * @param env the Ballerina environment of the publishing native call
     * @return the carrier map, or null if tracing is disabled or there is no active span to propagate
     */
    public static Map<String, String> getTraceContextHeaders(Environment env) {
        if (!ObserveUtils.isTracingEnabled()) {
            return null;
        }
        ObserverContext ctx = ObserveUtils.getObserverContextOfCurrentFrame(env);
        if (ctx == null) {
            return null;
        }
        return ObserveUtils.getContextProperties(ctx);
    }

    /**
     * Reads the trace-context entries (if any) out of a received Ballerina message's {@code properties} field.
     *
     * @param message the Ballerina message record
     * @return a (possibly empty) carrier map of the trace-context entries found
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> extractTraceContextHeaders(BMap<BString, Object> message) {
        Map<String, String> carrier = new HashMap<>();
        if (message == null) {
            return carrier;
        }
        Object propsObj = message.get(PROPERTIES_KEY);
        if (!(propsObj instanceof BMap)) {
            return carrier;
        }
        BMap<BString, Object> props = (BMap<BString, Object>) propsObj;
        for (String field : propagationFields()) {
            putIfPresent(carrier, props, field);
        }
        return carrier;
    }

    private static Collection<String> propagationFields() {
        TracersStore store = TracersStore.getInstance();
        if (!store.isInitialized()) {
            return Collections.emptyList();
        }
        return store.getPropagators().getTextMapPropagator().fields();
    }

    private static void putIfPresent(Map<String, String> carrier, BMap<BString, Object> props, String key) {
        Object value = props.get(StringUtils.fromString(key));
        if (value != null) {
            carrier.put(key, value.toString());
        }
    }

    private SolaceTracingUtil() {
    }
}
