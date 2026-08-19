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

package io.xlibb.solace.consumer;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessage;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.xlibb.solace.ModuleUtils;
import io.xlibb.solace.common.DestinationConverter;
import io.xlibb.solace.common.PropertyConverter;
import io.xlibb.solace.observability.SolaceTracingUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.xlibb.solace.common.Constants.NATIVE_MESSAGE;
import static io.xlibb.solace.common.MessageFieldConstants.APPLICATION_MESSAGE_ID_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.APPLICATION_MESSAGE_TYPE_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.CORRELATION_ID_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.DELIVERY_COUNT_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.DELIVERY_MODE_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.PAYLOAD_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.PRIORITY_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.PROPERTIES_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.RECEIVE_TIMESTAMP_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.REDELIVERED_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.REPLY_TO_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.SENDER_ID_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.SENDER_TIMESTAMP_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.SEQUENCE_NUMBER_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.TIME_TO_LIVE_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.USER_DATA_KEY;

/**
 * Converter for translating JCSMP XMLMessage to Ballerina Message record.
 */
public class MessageConverter {

    // Define union types for map values
    private static final UnionType MSG_PROPERTY_TYPE = TypeCreator.createUnionType(
            PredefinedTypes.TYPE_BOOLEAN, PredefinedTypes.TYPE_INT, PredefinedTypes.TYPE_BYTE,
            PredefinedTypes.TYPE_FLOAT, PredefinedTypes.TYPE_STRING);

    // Create typed MapType instances matching Ballerina types
    private static final MapType BALLERINA_MSG_PROPERTY_TYPE = TypeCreator.createMapType(
            "Property", MSG_PROPERTY_TYPE, ModuleUtils.getModule());

    /**
     * Converts a JCSMP XMLMessage to a Ballerina Message record.
     *
     * @param xmlMessage the JCSMP message to convert
     * @return the Ballerina Message record
     * @throws Exception if conversion fails
     */
    public static BMap<BString, Object> toBallerinaMessage(XMLMessage xmlMessage) throws Exception {
        // Create the Message record
        RecordType messageType = getMessageRecordType();
        BMap<BString, Object> message = ValueCreator.createRecordValue(messageType);

        // Extract and set payload
        byte[] payload = extractPayload(xmlMessage);
        message.put(PAYLOAD_KEY, ValueCreator.createArrayValue(payload));

        // Set delivery mode
        message.put(DELIVERY_MODE_KEY, StringUtils.fromString(xmlMessage.getDeliveryMode().toString()));

        // Set priority if present (getPriority returns -1 if not set)
        int priority = xmlMessage.getPriority();
        if (priority >= 0) {
            message.put(PRIORITY_KEY, (byte) priority);
        }

        // Set time to live
        long ttl = xmlMessage.getTimeToLive();
        if (ttl > 0) {
            message.put(TIME_TO_LIVE_KEY, (int) ttl);
        }

        // Set application message ID if present
        String appMsgId = xmlMessage.getApplicationMessageId();
        if (appMsgId != null) {
            message.put(APPLICATION_MESSAGE_ID_KEY, StringUtils.fromString(appMsgId));
        }

        // Set application message type if present
        String appMsgType = xmlMessage.getApplicationMessageType();
        if (appMsgType != null) {
            message.put(APPLICATION_MESSAGE_TYPE_KEY, StringUtils.fromString(appMsgType));
        }

        // Set correlation ID if present
        String correlationId = xmlMessage.getCorrelationId();
        if (correlationId != null) {
            message.put(CORRELATION_ID_KEY, StringUtils.fromString(correlationId));
        }

        // Set reply-to destination if present
        Destination replyTo = xmlMessage.getReplyTo();
        if (replyTo != null) {
            BMap<BString, Object> replyToMap = DestinationConverter.fromJCSMPDestination(replyTo);
            if (replyToMap != null) {
                message.put(REPLY_TO_KEY, replyToMap);
            }
        }

        // Set sender ID if present
        String senderId = xmlMessage.getSenderId();
        if (senderId != null) {
            message.put(SENDER_ID_KEY, StringUtils.fromString(senderId));
        }

        // Set sender timestamp if present
        Long senderTimestamp = xmlMessage.getSenderTimestamp();
        if (senderTimestamp != null) {
            message.put(SENDER_TIMESTAMP_KEY, senderTimestamp.intValue());
        }

        // Set receive timestamp if present (0 means not set)
        long receiveTimestamp = xmlMessage.getReceiveTimestamp();
        if (receiveTimestamp > 0) {
            message.put(RECEIVE_TIMESTAMP_KEY, (int) receiveTimestamp);
        }

        // Set sequence number if present
        Long sequenceNumber = xmlMessage.getSequenceNumber();
        if (sequenceNumber != null) {
            message.put(SEQUENCE_NUMBER_KEY, sequenceNumber.intValue());
        }

        // Set redelivered flag
        message.put(REDELIVERED_KEY, xmlMessage.getRedelivered());

        try {
            int deliveryCount = xmlMessage.getDeliveryCount();
            if (deliveryCount > 0) {
                message.put(DELIVERY_COUNT_KEY, deliveryCount);
            }
        } catch (UnsupportedOperationException ignored) {
        }

        // Set properties if present.
        SDTMap sdtProperties = xmlMessage.getProperties();
        BMap<BString, Object> properties = sdtProperties != null
                ? PropertyConverter.sdtMapToBallerina(sdtProperties, BALLERINA_MSG_PROPERTY_TYPE)
                : ValueCreator.createMapValue(BALLERINA_MSG_PROPERTY_TYPE);
        SolaceTracingUtil.surfaceNativeTraceContext(xmlMessage, properties);
        if (!properties.isEmpty()) {
            message.put(PROPERTIES_KEY, properties);
        }

        // Set user data if present
        if (xmlMessage.hasUserData()) {
            byte[] userData = xmlMessage.getUserData();
            if (userData != null && userData.length > 0) {
                message.put(USER_DATA_KEY, ValueCreator.createArrayValue(userData));
            }
        }

        // Store native message for acknowledgement operations
        message.addNativeData(NATIVE_MESSAGE, xmlMessage);

        return message;
    }

    /**
     * Extracts the native XMLMessage from a Ballerina Message record.
     *
     * @param message the Ballerina Message record
     * @return the native XMLMessage, or null if not found
     */
    public static XMLMessage extractNativeMessage(BMap<BString, Object> message) {
        Object nativeMsg = message.getNativeData(NATIVE_MESSAGE);
        if (nativeMsg instanceof XMLMessage) {
            return (XMLMessage) nativeMsg;
        }
        return null;
    }

    private static byte[] extractPayload(XMLMessage xmlMessage) throws Exception {
        if (xmlMessage instanceof TextMessage textMessage) {
            String text = textMessage.getText();
            if (text != null) {
                return text.getBytes(StandardCharsets.UTF_8);
            }
        } else if (xmlMessage instanceof BytesMessage bytesMessage) {
            return bytesMessage.getData();
        } else if (xmlMessage instanceof MapMessage mapMessage) {
            SDTMap map = mapMessage.getMap();
            if (map != null && !map.isEmpty()) {
                String jsonString = PropertyConverter.sdtMapToJson(map);
                return jsonString.getBytes(StandardCharsets.UTF_8);
            }
        } else {
            ByteBuffer buf = xmlMessage.getAttachmentByteBuffer();
            byte[] content = new byte[buf.remaining()];
            buf.get(content);
            return content;
        }
        return new byte[0];
    }

    private static RecordType getMessageRecordType() {
        return (RecordType) ValueCreator.createRecordValue(
                ModuleUtils.getModule(),
                "Message"
        ).getType();
    }
}
