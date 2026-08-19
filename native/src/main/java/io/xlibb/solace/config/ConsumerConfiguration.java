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

package io.xlibb.solace.config;

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

/**
 * Consumer configuration for synchronous (pull-based) message consumption via MessageConsumer. Composes
 * CommonConnectionConfiguration and a ConsumerSubscriptionConfig (queue or topic).
 *
 * @param connectionConfig   the common connection configuration
 * @param subscriptionConfig the consumer subscription configuration (queue or topic)
 */
public record ConsumerConfiguration(
        ConnectionConfiguration connectionConfig,
        ConsumerSubscriptionConfig subscriptionConfig) {

    private static final BString SUBSCRIPTION_CONFIG_KEY = StringUtils.fromString("subscriptionConfig");

    /**
     * Creates a ConsumerConfiguration from a Ballerina map record. The map contains both connection configuration
     * fields and subscription configuration.
     *
     * @param config the configuration map
     */
    public ConsumerConfiguration(BMap<BString, Object> config) {
        this(
                new ConnectionConfiguration(config),
                getSubscriptionConfig((BMap<BString, Object>) config.getMapValue(SUBSCRIPTION_CONFIG_KEY))
        );
    }

    private static ConsumerSubscriptionConfig getSubscriptionConfig(BMap<BString, Object> subscriptionConfigMap) {
        if (subscriptionConfigMap == null) {
            throw new IllegalArgumentException("Subscription configuration is required");
        }

        ConsumerSubscriptionConfig config = ConsumerSubscriptionConfig.fromBMap(subscriptionConfigMap);

        if (config instanceof TopicConsumerConfig topicConfig) {
            topicConfig.validate();
        }

        return config;
    }
}
