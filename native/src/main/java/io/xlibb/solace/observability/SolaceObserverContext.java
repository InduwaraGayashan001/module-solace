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

import io.ballerina.runtime.observability.ObserverContext;

/**
 * Extension of ObserverContext for Solace, pre-populating the listener.name tag.
 */
public class SolaceObserverContext extends ObserverContext {

    private SolaceObserverContext() {
        addTag(SolaceObservabilityConstants.TAG_KEY_LISTENER_NAME, SolaceObservabilityConstants.CONNECTOR_NAME);
    }

    SolaceObserverContext(String context) {
        this();
        addTag(SolaceObservabilityConstants.TAG_KEY_CONTEXT, context);
    }

    public SolaceObserverContext(String context, String url) {
        this(context);
        addTag(SolaceObservabilityConstants.TAG_KEY_URL, defaulted(url));
    }

    public SolaceObserverContext(String context, String url, String destination) {
        this(context, url);
        addTag(SolaceObservabilityConstants.TAG_KEY_DESTINATION, defaulted(destination));
    }

    public SolaceObserverContext withVpn(String vpn) {
        addTag(SolaceObservabilityConstants.TAG_KEY_VPN, defaulted(vpn));
        return this;
    }

    public SolaceObserverContext withDestinationKind(String kind) {
        addTag(SolaceObservabilityConstants.TAG_KEY_DESTINATION_KIND, defaulted(kind));
        return this;
    }

    public SolaceObserverContext withDeliveryMode(String mode) {
        addTag(SolaceObservabilityConstants.TAG_KEY_DELIVERY_MODE, defaulted(mode));
        return this;
    }

    SolaceObserverContext withTag(String key, String value) {
        addTag(key, defaulted(value));
        return this;
    }

    private static String defaulted(String value) {
        return value == null || value.isBlank() ? SolaceObservabilityConstants.UNKNOWN : value;
    }
}
