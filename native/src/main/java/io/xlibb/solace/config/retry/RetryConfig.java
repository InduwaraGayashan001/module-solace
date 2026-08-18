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

package io.xlibb.solace.config.retry;

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.math.BigDecimal;

/**
 * Retry configuration for connection attempts.
 *
 * @param connectRetries        number of times to retry connecting during initial connection
 * @param connectRetriesPerHost number of connection retries per host
 * @param reconnectRetries      number of times to retry reconnecting after connection loss
 * @param reconnectRetryWait    time to wait between reconnection attempts in milliseconds
 */
public record RetryConfig(int connectRetries, int connectRetriesPerHost, int reconnectRetries,
                          long reconnectRetryWait) {

    private static final BString CONNECT_RETRIES_KEY = StringUtils.fromString("connectRetries");
    private static final BString CONNECT_RETRIES_PER_HOST_KEY = StringUtils.fromString("connectRetriesPerHost");
    private static final BString RECONNECT_RETRIES_KEY = StringUtils.fromString("reconnectRetries");
    private static final BString RECONNECT_RETRY_WAIT_KEY = StringUtils.fromString("reconnectRetryWait");

    /**
     * Creates a RetryConfig from a Ballerina map record.
     */
    public RetryConfig(BMap<BString, Object> config) {
        this(
                Math.toIntExact(config.getIntValue(CONNECT_RETRIES_KEY)),
                Math.toIntExact(config.getIntValue(CONNECT_RETRIES_PER_HOST_KEY)),
                Math.toIntExact(config.getIntValue(RECONNECT_RETRIES_KEY)),
                decimalToMillis(((BDecimal) config.get(RECONNECT_RETRY_WAIT_KEY)).decimalValue())
        );
    }

    private static long decimalToMillis(BigDecimal seconds) {
        return seconds.multiply(BigDecimal.valueOf(1000)).longValue();
    }
}
