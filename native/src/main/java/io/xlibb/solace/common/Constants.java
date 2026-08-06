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

package io.xlibb.solace.common;

/**
 * Constants used to store native data in Ballerina objects.
 */
public class Constants {

    public static final String NATIVE_SESSION = "native.session";
    public static final String NATIVE_TX_SESSION = "native.tx.session";
    public static final String NATIVE_FLOW = "native.flow";
    public static final String NATIVE_CONSUMER = "native.consumer";
    public static final String NATIVE_SUBSCRIPTION_TYPE = "native.subscription.type";
    public static final String NATIVE_TRANSACTED = "native.transacted";
    public static final String NATIVE_CLOSED = "native.closed";
    public static final String NATIVE_PRODUCER = "native.producer";
    public static final String NATIVE_MESSAGE = "native.message";
    public static final String NATIVE_URL = "native.url";
    public static final String NATIVE_DESTINATION = "native.destination";
    public static final String NATIVE_VPN = "native.vpn";
    public static final String NATIVE_DESTINATION_KIND = "native.destination.kind";
    public static final String NATIVE_EVENT_HANDLER = "native.session.event.handler";

    // Listener-specific native data keys
    public static final String NATIVE_RUNTIME = "native.runtime";
    public static final String NATIVE_SERVICES = "native.services";
    public static final String NATIVE_STARTED = "native.started";

}
