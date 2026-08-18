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

import com.solace.messaging.trace.propagation.MessageTracingSupport;
import com.solace.messaging.trace.propagation.TraceContext;
import com.solace.messaging.trace.propagation.TraceContextSetter;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.XMLMessage;
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

    // W3C trace-context field names, matching what the OpenTelemetry propagator declares.
    private static final String TRACEPARENT = "traceparent";
    private static final String TRACESTATE = "tracestate";
    private static final String W3C_VERSION = "00";
    private static final String SAMPLED_FLAGS = "01";
    private static final String NOT_SAMPLED_FLAGS = "00";
    private static final String TRACEPARENT_DELIMITER = "-";
    private static final int TRACE_ID_LENGTH = 16;
    private static final int SPAN_ID_LENGTH = 8;
    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

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

    /**
     * Writes the publishing span's trace-context onto an outbound message.
     * 
     * @param env        the Ballerina environment of the publishing native call
     * @param xmlMessage the outbound JCSMP message
     * @throws Exception if the message's property map cannot be written
     */
    public static void applyTraceContext(Environment env, XMLMessage xmlMessage) throws Exception {
        Map<String, String> carrier = getTraceContextHeaders(env);
        if (carrier == null || carrier.isEmpty() || xmlMessage == null) {
            return;
        }
        String traceParent = carrier.get(TRACEPARENT);
        String traceState = carrier.get(TRACESTATE);

        setNativeTraceContext(xmlMessage, traceParent, traceState);

        SDTMap properties = xmlMessage.getProperties();
        if (properties == null) {
            properties = JCSMPFactory.onlyInstance().createMap();
        }
        if (traceParent != null && !properties.containsKey(TRACEPARENT)) {
            properties.putString(TRACEPARENT, traceParent);
        }
        if (traceState != null && !traceState.isEmpty() && !properties.containsKey(TRACESTATE)) {
            properties.putString(TRACESTATE, traceState);
        }
        xmlMessage.setProperties(properties);
    }

    private static void setNativeTraceContext(XMLMessage xmlMessage, String traceParent, String traceState) {
        if (traceParent == null || !(xmlMessage instanceof MessageTracingSupport tracing)) {
            return;
        }
        // <version>-<32 hex trace id>-<16 hex span id>-<2 hex flags>
        String[] parts = traceParent.split(TRACEPARENT_DELIMITER);
        if (parts.length < 4 || parts[1].length() != TRACE_ID_LENGTH * 2
                || parts[2].length() != SPAN_ID_LENGTH * 2 || parts[3].length() < 2) {
            return;
        }
        byte[] traceId = fromHex(parts[1]);
        byte[] spanId = fromHex(parts[2]);
        byte[] flags = fromHex(parts[3].substring(0, 2));
        if (traceId.length == 0 || spanId.length == 0 || flags.length == 0) {
            return;
        }
        TraceContextSetter setter = tracing.contextSetter();
        setter.setTraceIdBytes16(traceId);
        setter.setSpanIdBytes8(spanId);
        setter.setSampled((flags[0] & 0x01) != 0);
        if (traceState != null && !traceState.isEmpty()) {
            setter.setTraceState(traceState);
        }
    }

    private static byte[] fromHex(String hex) {
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            int high = Character.digit(hex.charAt(i * 2), 16);
            int low = Character.digit(hex.charAt(i * 2 + 1), 16);
            if (high < 0 || low < 0) {
                return new byte[0];
            }
            out[i] = (byte) ((high << 4) | low);
        }
        return out;
    }

    /**
     * Copies the trace-context the broker attached to a message into its user properties.
     *
     * @param xmlMessage the received JCSMP message
     * @param properties the Ballerina property map being built for it, mutated in place
     */
    public static void surfaceNativeTraceContext(XMLMessage xmlMessage, BMap<BString, Object> properties) {
        if (xmlMessage == null || properties == null) {
            return;
        }
        Collection<String> fields = propagationFields();
        if (fields.isEmpty() || !fields.contains(TRACEPARENT)) {
            return;
        }
        if (!(xmlMessage instanceof MessageTracingSupport tracing)) {
            return;
        }
        TraceContext context = firstUsable(tracing.transportContext(), tracing.creationContext());
        if (context == null) {
            return;
        }
        properties.put(StringUtils.fromString(TRACEPARENT), StringUtils.fromString(toTraceParent(context)));
        String traceState = context.getTraceState();
        if (traceState != null && !traceState.isEmpty() && fields.contains(TRACESTATE)) {
            properties.put(StringUtils.fromString(TRACESTATE), StringUtils.fromString(traceState));
        }
    }

    private static TraceContext firstUsable(TraceContext... contexts) {
        for (TraceContext context : contexts) {
            if (context != null && isSet(context.getTraceIdBytes16(), TRACE_ID_LENGTH)
                    && isSet(context.getSpanIdBytes8(), SPAN_ID_LENGTH)) {
                return context;
            }
        }
        return null;
    }

    private static boolean isSet(byte[] id, int expectedLength) {
        if (id == null || id.length != expectedLength) {
            return false;
        }
        for (byte octet : id) {
            if (octet != 0) {
                return true;
            }
        }
        return false;
    }

    private static String toTraceParent(TraceContext context) {
        return String.join(TRACEPARENT_DELIMITER,
                W3C_VERSION,
                toHex(context.getTraceIdBytes16()),
                toHex(context.getSpanIdBytes8()),
                context.isSampled() ? SAMPLED_FLAGS : NOT_SAMPLED_FLAGS);
    }

    private static String toHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte octet : bytes) {
            hex.append(HEX_DIGITS[(octet >> 4) & 0xF]).append(HEX_DIGITS[octet & 0xF]);
        }
        return hex.toString();
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
