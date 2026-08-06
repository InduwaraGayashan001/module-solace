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

import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import io.ballerina.runtime.api.values.BObject;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.xlibb.solace.common.Constants.NATIVE_EVENT_HANDLER;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.EVENT_DOWN;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.EVENT_RECONNECTED;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.EVENT_RECONNECTING;

/**
 * Translates JCSMP session events into connectivity metrics.
 */
public class SolaceSessionEventHandler implements SessionEventHandler {

    private final String context;
    private final String url;
    private final String vpn;
    private final AtomicBoolean connected = new AtomicBoolean(false);

    public SolaceSessionEventHandler(String context, String url, String vpn) {
        this.context = context;
        this.url = url;
        this.vpn = vpn;
    }

    /**
     * Records that the session is up.
     */
    public void markConnected() {
        if (connected.compareAndSet(false, true)) {
            SolaceMetricsUtil.adjustConnectionsUp(context, url, vpn, 1);
        }
    }

    /**
     * Records that the session is no longer usable - either because the broker dropped it or because the application
     * closed it.
     */
    public void markDisconnected() {
        if (connected.compareAndSet(true, false)) {
            SolaceMetricsUtil.adjustConnectionsUp(context, url, vpn, -1);
        }
    }

    /**
     * Marks the session owned by {@code object} disconnected.  */
    public static void markDisconnected(BObject object) {
        if (object == null) {
            return;
        }
        Object handler = object.getNativeData(NATIVE_EVENT_HANDLER);
        if (handler instanceof SolaceSessionEventHandler sessionHandler) {
            sessionHandler.markDisconnected();
        }
    }

    @Override
    public void handleEvent(SessionEventArgs event) {
        try {
            switch (event.getEvent()) {
                case RECONNECTING -> {
                    SolaceMetricsUtil.reportSessionEvent(context, url, vpn, EVENT_RECONNECTING);
                    markDisconnected();
                }
                case RECONNECTED -> {
                    SolaceMetricsUtil.reportSessionEvent(context, url, vpn, EVENT_RECONNECTED);
                    markConnected();
                }
                case DOWN_ERROR -> {
                    SolaceMetricsUtil.reportSessionEvent(context, url, vpn, EVENT_DOWN);
                    markDisconnected();
                    SolaceMetricsUtil.reportConnectionError(context, url, vpn);
                }
                default -> {
                    // SUBSCRIPTION_ERROR, VIRTUAL_ROUTER_NAME_CHANGED and friends are not connectivity transitions.
                }
            }
        } catch (Throwable ignored) {
            // Never let a metrics failure propagate into the JCSMP reactor thread.
        }
    }
}
