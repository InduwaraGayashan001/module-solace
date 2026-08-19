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
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.xlibb.solace.config.auth.AuthConfig;
import io.xlibb.solace.config.auth.BasicAuthConfig;
import io.xlibb.solace.config.auth.KerberosConfig;
import io.xlibb.solace.config.auth.OAuth2Config;
import io.xlibb.solace.config.retry.RetryConfig;
import io.xlibb.solace.config.ssl.SecureSocketConfig;

import java.math.BigDecimal;

/**
 * Connection configuration for Solace JCSMP connections. Maps to CommonConnectionConfiguration in Ballerina types.bal.
 *
 * @param vpnName                    message VPN to connect to (default: "default")
 * @param clientName                 client identifier, or null for auto-generated
 * @param clientDescription          description for application client
 * @param localAddress               local interface IP address to bind, or null
 * @param connectionTimeout          maximum time in milliseconds for connection attempt
 * @param readTimeout                maximum time in milliseconds for reading replies
 * @param compressionLevel           ZLIB compression level (0-9, 0 = disabled)
 * @param transacted                 true to enable transacted messaging
 * @param generateReceiveTimestamps  whether to generate receive timestamps on incoming messages
 * @param generateSendTimestamps     whether to generate send timestamps on outgoing messages
 * @param generateSequenceNumbers    whether to generate sequence numbers on outgoing messages
 * @param calculateMessageExpiration whether to calculate message expiration from TTL
 * @param auth                       authentication configuration, or null
 * @param retryConfig                retry configuration, or null
 * @param secureSocket               SSL/TLS configuration, or null
 */
public record ConnectionConfiguration(
        String vpnName,
        String clientName,
        String clientDescription,
        String localAddress,
        long connectionTimeout,
        long readTimeout,
        int compressionLevel,
        boolean transacted,
        boolean generateReceiveTimestamps,
        boolean generateSendTimestamps,
        boolean generateSequenceNumbers,
        boolean calculateMessageExpiration,
        AuthConfig auth,
        RetryConfig retryConfig,
        SecureSocketConfig secureSocket) {

    private static final BString VPN_NAME_KEY = StringUtils.fromString("vpnName");
    private static final BString CLIENT_NAME_KEY = StringUtils.fromString("clientName");
    private static final BString CLIENT_DESCRIPTION_KEY = StringUtils.fromString("clientDescription");
    private static final BString LOCAL_ADDRESS_KEY = StringUtils.fromString("localAddress");
    private static final BString CONNECTION_TIMEOUT_KEY = StringUtils.fromString("connectionTimeout");
    private static final BString READ_TIMEOUT_KEY = StringUtils.fromString("readTimeout");
    private static final BString COMPRESSION_LEVEL_KEY = StringUtils.fromString("compressionLevel");
    private static final BString TRANSACTED_KEY = StringUtils.fromString("transacted");
    private static final BString GENERATE_RECEIVE_TIMESTAMPS_KEY = StringUtils.fromString("generateReceiveTimestamps");
    private static final BString GENERATE_SEND_TIMESTAMPS_KEY = StringUtils.fromString("generateSendTimestamps");
    private static final BString GENERATE_SEQUENCE_NUMBERS_KEY = StringUtils.fromString("generateSequenceNumbers");
    private static final BString CALCULATE_MESSAGE_EXPIRATION_KEY =
            StringUtils.fromString("calculateMessageExpiration");
    private static final BString AUTH_KEY = StringUtils.fromString("auth");
    private static final BString RETRY_CONFIG_KEY = StringUtils.fromString("retryConfig");
    private static final BString SECURE_SOCKET_KEY = StringUtils.fromString("secureSocket");

    /**
     * Creates a ConnectionConfiguration from a Ballerina map record.
     *
     * @param config the Ballerina configuration map
     */
    public ConnectionConfiguration(BMap<BString, Object> config) {
        this(
                config.getStringValue(VPN_NAME_KEY).getValue(),
                getOptionalString(config, CLIENT_NAME_KEY),
                config.getStringValue(CLIENT_DESCRIPTION_KEY).getValue(),
                getOptionalString(config, LOCAL_ADDRESS_KEY),
                decimalToMillis(((BDecimal) config.get(CONNECTION_TIMEOUT_KEY)).decimalValue()),
                decimalToMillis(((BDecimal) config.get(READ_TIMEOUT_KEY)).decimalValue()),
                Math.toIntExact(config.getIntValue(COMPRESSION_LEVEL_KEY)),
                config.getBooleanValue(TRANSACTED_KEY),
                config.getBooleanValue(GENERATE_RECEIVE_TIMESTAMPS_KEY),
                config.getBooleanValue(GENERATE_SEND_TIMESTAMPS_KEY),
                config.getBooleanValue(GENERATE_SEQUENCE_NUMBERS_KEY),
                config.getBooleanValue(CALCULATE_MESSAGE_EXPIRATION_KEY),
                getAuthConfig(config),
                getRetryConfig(config),
                getSecureSocketConfig(config)
        );
    }

    private static String getOptionalString(BMap<BString, Object> map, BString key) {
        if (map.containsKey(key)) {
            Object value = map.get(key);
            if (value != null) {
                return value.toString();
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static AuthConfig getAuthConfig(BMap<BString, Object> config) {
        if (!config.containsKey(AUTH_KEY)) {
            return null;
        }
        Object authObj = config.get(AUTH_KEY);
        if (authObj == null) {
            return null;
        }
        BMap<BString, Object> authMap = (BMap<BString, Object>) authObj;
        return createAuthConfig(authMap);
    }

    private static AuthConfig createAuthConfig(BMap<BString, Object> authMap) {
        BString usernameKey = StringUtils.fromString("username");
        BString accessTokenKey = StringUtils.fromString("accessToken");
        BString idTokenKey = StringUtils.fromString("idToken");
        BString serviceNameKey = StringUtils.fromString("serviceName");

        if (authMap.containsKey(usernameKey)) {
            return new BasicAuthConfig(authMap);
        } else if (authMap.containsKey(accessTokenKey) || authMap.containsKey(idTokenKey)) {
            return new OAuth2Config(authMap);
        } else if (authMap.containsKey(serviceNameKey)) {
            return new KerberosConfig(authMap);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static RetryConfig getRetryConfig(BMap<BString, Object> config) {
        if (config.containsKey(RETRY_CONFIG_KEY)) {
            Object retryObj = config.get(RETRY_CONFIG_KEY);
            if (retryObj != null) {
                BMap<BString, Object> retryMap = (BMap<BString, Object>) retryObj;
                return new RetryConfig(retryMap);
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static SecureSocketConfig getSecureSocketConfig(BMap<BString, Object> config) {
        if (config.containsKey(SECURE_SOCKET_KEY)) {
            Object secureSocketObj = config.get(SECURE_SOCKET_KEY);
            if (secureSocketObj != null) {
                BMap<BString, Object> secureSocketMap = (BMap<BString, Object>) secureSocketObj;
                return new SecureSocketConfig(secureSocketMap);
            }
        }
        return null;
    }

    private static long decimalToMillis(BigDecimal seconds) {
        return seconds.multiply(BigDecimal.valueOf(1000)).longValue();
    }
}
