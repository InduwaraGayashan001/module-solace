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

package io.xlibb.solace.producer;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import io.xlibb.solace.observability.SolaceMetricsUtil;

/**
 * Handler for JCSMP streaming publish events.
 * 
 */
public class PublishEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {

    private final String url;
    private final String vpn;

    PublishEventHandler(String url, String vpn) {
        this.url = url;
        this.vpn = vpn;
    }

    /**
     * Called when a publisher acknowledgement is received for a guaranteed delivery message.
     *
     * @param key The correlation key of the message being acknowledged
     */
    @Override
    public void responseReceivedEx(Object key) {
        record(true);
    }

    /**
     * Called when an error occurs during message publishing.
     *
     * @param key       The correlation key of the message with which the error condition is associated
     * @param cause     The error condition
     * @param timestamp The time of the error given by `System.currentTimeMillis()`
     */
    @Override
    public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
        record(false);
    }

    private void record(boolean accepted) {
        try {
            SolaceMetricsUtil.reportPublishConfirm(url, vpn, accepted);
        } catch (Throwable ignored) {
            // A metrics failure must never disturb the JCSMP dispatch thread.
        }
    }
}
