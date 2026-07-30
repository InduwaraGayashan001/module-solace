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

package io.xlibb.solace.caller;

import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.xlibb.solace.common.CommonUtils;
import io.xlibb.solace.consumer.MessageConverter;
import io.xlibb.solace.observability.SolaceMetricsUtil;

import java.util.logging.Logger;

import static io.xlibb.solace.common.Constants.NATIVE_TX_SESSION;
import static io.xlibb.solace.observability.SolaceMetricsUtil.reportConsumerFailure;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_ACKNOWLEDGE;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_COMMIT;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_NACK;
import static io.xlibb.solace.observability.SolaceObservabilityConstants.ERROR_TYPE_ROLLBACK;

/**
 * Caller actions - interop for the Ballerina Solace {@code Caller} supplied to a service's {@code onMessage} method.
 * Provides explicit acknowledgement / negative-acknowledgement of the received message and transaction control on the
 * listener's transacted session.
 */
public class CallerActions {

    private static final Logger LOGGER = Logger.getLogger(CallerActions.class.getName());
    private static final String TRANSACTED_SETTLE_WARNING =
            "%s is ignored on a transacted listener connection; message settlement is controlled by "
                    + "commit()/rollback().";

    private static boolean isTransacted(BObject caller) {
        return caller.getNativeData(NATIVE_TX_SESSION) != null;
    }

    /**
     * Acknowledge a message (CLIENT_ACK mode).
     *
     * @param caller  the Ballerina caller object
     * @param message the Ballerina message to acknowledge
     * @return null on success, BError on failure
     */
    public static BError ack(BObject caller, BMap<BString, Object> message) {
        if (isTransacted(caller)) {
            LOGGER.warning(String.format(TRANSACTED_SETTLE_WARNING, "ack()"));
            return null;
        }
        try {
            XMLMessage nativeMessage = MessageConverter.extractNativeMessage(message);
            if (nativeMessage == null) {
                return reportConsumerFailure(caller, ERROR_TYPE_ACKNOWLEDGE,
                        "Cannot acknowledge: native message not found");
            }
            Object result = CommonUtils.executeBlocking(nativeMessage::ackMessage);
            if (result instanceof BError bError) {
                SolaceMetricsUtil.reportConsumerError(caller, ERROR_TYPE_ACKNOWLEDGE);
                return bError;
            }
            SolaceMetricsUtil.reportAck(caller);
            return null;
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(caller, ERROR_TYPE_ACKNOWLEDGE);
            return CommonUtils.createError("Failed to acknowledge message", e);
        }
    }

    /**
     * Negatively acknowledge a message (NACK).
     *
     * @param caller  the Ballerina caller object
     * @param message the Ballerina message to NACK
     * @param requeue if true, use FAILED outcome (requeue); if false, use REJECTED outcome (DMQ)
     * @return null on success, BError on failure
     */
    public static BError nack(BObject caller, BMap<BString, Object> message, boolean requeue) {
        if (isTransacted(caller)) {
            LOGGER.warning(String.format(TRANSACTED_SETTLE_WARNING, "nack()"));
            return null;
        }
        try {
            XMLMessage nativeMessage = MessageConverter.extractNativeMessage(message);
            if (nativeMessage == null) {
                return reportConsumerFailure(caller, ERROR_TYPE_NACK, "Cannot NACK: native message not found");
            }
            Object result = CommonUtils.executeBlocking(() -> {
                XMLMessage.Outcome outcome = requeue ? XMLMessage.Outcome.FAILED : XMLMessage.Outcome.REJECTED;
                nativeMessage.settle(outcome);
                return null;
            });
            if (result instanceof BError bError) {
                SolaceMetricsUtil.reportConsumerError(caller, ERROR_TYPE_NACK);
                return bError;
            }
            SolaceMetricsUtil.reportNack(caller, requeue);
            return null;
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(caller, ERROR_TYPE_NACK);
            return CommonUtils.createError("Failed to NACK message", e);
        }
    }

    /**
     * Commit the current transaction. Only valid when the listener connection is transacted.
     *
     * @param caller the Ballerina caller object
     * @return null on success, BError on failure
     */
    public static BError commit(BObject caller) {
        TransactedSession txSession = (TransactedSession) caller.getNativeData(NATIVE_TX_SESSION);
        if (txSession == null) {
            return reportConsumerFailure(caller, ERROR_TYPE_COMMIT,
                    "commit() can only be called when the listener connection is transacted. "
                            + "Set transacted = true on the listener configuration to enable transactions.");
        }
        try {
            Object result = CommonUtils.executeBlocking(txSession::commit);
            if (result instanceof BError bError) {
                // executeBlocking returns a broker-side failure as a BError rather than throwing, so this - not the
                // catch below - is the path a real commit failure takes.
                SolaceMetricsUtil.reportConsumerError(caller, ERROR_TYPE_COMMIT);
                return bError;
            }
            return null;
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(caller, ERROR_TYPE_COMMIT);
            return CommonUtils.createError("Failed to commit transaction", e);
        }
    }

    /**
     * Rollback the current transaction. Only valid when the listener connection is transacted.
     *
     * @param caller the Ballerina caller object
     * @return null on success, BError on failure
     */
    public static BError rollback(BObject caller) {
        TransactedSession txSession = (TransactedSession) caller.getNativeData(NATIVE_TX_SESSION);
        if (txSession == null) {
            return reportConsumerFailure(caller, ERROR_TYPE_ROLLBACK,
                    "rollback() can only be called when the listener connection is transacted. "
                            + "Set transacted = true on the listener configuration to enable transactions.");
        }
        try {
            Object result = CommonUtils.executeBlocking(txSession::rollback);
            if (result instanceof BError bError) {
                // As with commit(), a broker-side failure arrives here as a BError, not as an exception.
                SolaceMetricsUtil.reportConsumerError(caller, ERROR_TYPE_ROLLBACK);
                return bError;
            }
            return null;
        } catch (Exception e) {
            SolaceMetricsUtil.reportConsumerError(caller, ERROR_TYPE_ROLLBACK);
            return CommonUtils.createError("Failed to rollback transaction", e);
        }
    }
}
