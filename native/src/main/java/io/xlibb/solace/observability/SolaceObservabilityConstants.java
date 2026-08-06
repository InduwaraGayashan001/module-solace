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

/**
 * Constants for Solace observability (metrics and tracing).
 */
public class SolaceObservabilityConstants {

    static final String CONNECTOR_NAME = "solace";

    static final String[] METRIC_PUBLISHERS = {"publishers", "Number of currently active publishers"};
    static final String[] METRIC_CONSUMERS = {"consumers", "Number of currently active consumers"};
    static final String[] METRIC_PUBLISHED = {"published", "Number of messages published"};
    static final String[] METRIC_PUBLISHED_SIZE = {"published_size", "Total size in bytes of messages published"};
    static final String[] METRIC_CONSUMED = {"consumed", "Number of messages consumed"};
    static final String[] METRIC_CONSUMED_SIZE = {"consumed_size", "Total size in bytes of messages consumed"};
    static final String[] METRIC_ERRORS = {"errors", "Number of errors"};

    static final String[] METRIC_ACKS = {"acks", "Number of messages acknowledged"};
    static final String[] METRIC_NACKS = {"nacks", "Number of messages negatively acknowledged"};
    static final String[] METRIC_REDELIVERED =
            {"redelivered", "Number of consumed messages flagged as redelivered by the broker"};
    static final String[] METRIC_EMPTY_RECEIVES =
            {"empty_receives", "Number of receive calls that returned no message"};
    static final String[] METRIC_PUBLISH_CONFIRMS =
            {"publish_confirms", "Number of broker publish acknowledgements for guaranteed messages"};
    static final String[] METRIC_RECONNECTS = {"reconnects", "Number of session connectivity events"};
    static final String[] METRIC_CONNECTIONS_UP =
            {"connections_up", "Number of sessions currently connected to the broker"};
    static final String[] METRIC_PUBLISH_DURATION =
            {"publish_duration_seconds", "Time taken by a publish call, in seconds"};
    static final String[] METRIC_PROCESS_DURATION =
            {"process_duration_seconds", "Time taken to dispatch a message to the service, in seconds"};

    static final String TAG_KEY_URL = "url";
    static final String TAG_KEY_DESTINATION = "destination";
    static final String TAG_KEY_ERROR_TYPE = "error_type";
    static final String TAG_KEY_CONTEXT = "context";
    static final String TAG_KEY_LISTENER_NAME = "listener.name";
    static final String TAG_KEY_VPN = "vpn";
    static final String TAG_KEY_DESTINATION_KIND = "destination_kind";
    static final String TAG_KEY_DELIVERY_MODE = "delivery_mode";
    static final String TAG_KEY_OUTCOME = "outcome";
    static final String TAG_KEY_RESULT = "result";
    static final String TAG_KEY_EVENT = "event";

    public static final String ERROR_TYPE_CONNECTION = "connection";
    public static final String ERROR_TYPE_PUBLISH = "publish";
    public static final String ERROR_TYPE_CLOSE = "close";
    public static final String ERROR_TYPE_RECEIVE = "receive";
    public static final String ERROR_TYPE_ACKNOWLEDGE = "acknowledge";
    public static final String ERROR_TYPE_NACK = "nack";
    public static final String ERROR_TYPE_COMMIT = "commit";
    public static final String ERROR_TYPE_ROLLBACK = "rollback";
    public static final String ERROR_TYPE_DISPATCH = "dispatch";

    public static final String CONTEXT_PRODUCER = "producer";
    public static final String CONTEXT_CONSUMER = "consumer";
    public static final String CONTEXT_LISTENER = "listener";

    public static final String DESTINATION_KIND_QUEUE = "queue";
    public static final String DESTINATION_KIND_TOPIC = "topic";

    public static final String NACK_OUTCOME_REQUEUE = "requeue";
    public static final String NACK_OUTCOME_DMQ = "dmq";

    public static final String CONFIRM_ACCEPTED = "accepted";
    public static final String CONFIRM_REJECTED = "rejected";

    public static final String EVENT_RECONNECTING = "reconnecting";
    public static final String EVENT_RECONNECTED = "reconnected";
    public static final String EVENT_DOWN = "down";

    public static final String UNKNOWN = "unknown";

    private SolaceObservabilityConstants() {
    }
}
