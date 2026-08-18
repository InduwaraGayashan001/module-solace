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

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.xlibb.solace.common.DestinationConverter;
import io.xlibb.solace.common.PropertyConverter;

import static io.xlibb.solace.common.MessageFieldConstants.APPLICATION_MESSAGE_ID_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.APPLICATION_MESSAGE_TYPE_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.CORRELATION_ID_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.DELIVERY_MODE_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.PAYLOAD_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.PRIORITY_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.PROPERTIES_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.REPLY_TO_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.SENDER_ID_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.SENDER_TIMESTAMP_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.SEQUENCE_NUMBER_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.TIME_TO_LIVE_KEY;
import static io.xlibb.solace.common.MessageFieldConstants.USER_DATA_KEY;

/**
 * Converter for translating Ballerina messages to JCSMP message types.
 */
public class MessageConverter {

    /**
     * Converts a Ballerina Message to a JCSMP XMLMessage.
     *
     * @param producer the JCSMP message producer (used for message creation)
     * @param message  the Ballerina Message record
     * @return the JCSMP XMLMessage with all fields set
     * @throws Exception if conversion fails
     */
    public static XMLMessage toJCSMPMessage(XMLMessageProducer producer, BMap<BString, Object> message)
            throws Exception {
        Object payload = message.get(PAYLOAD_KEY);

        XMLMessage jcsmpMessage;
        if (payload instanceof BArray) {
            jcsmpMessage = toByteMessage(producer, ((BArray) payload).getBytes());
        } else {
            throw new Exception(
                    "Unsupported payload type: " + (payload != null ? payload.getClass().getName() : "null"));
        }

        // Set all message fields from Ballerina Message record
        setMessageFields(jcsmpMessage, message);

        return jcsmpMessage;
    }

    private static void setMessageFields(XMLMessage jcsmpMessage, BMap<BString, Object> message) throws SDTException {
        // Delivery mode
        String deliveryMode = message.getStringValue(DELIVERY_MODE_KEY).getValue();
        if (deliveryMode != null) {
            jcsmpMessage.setDeliveryMode(DeliveryMode.valueOf(deliveryMode));
        }

        // Priority (0-255)
        Long priority = message.getIntValue(PRIORITY_KEY);
        if (priority != null) {
            int priorityValue = priority.intValue();
            jcsmpMessage.setPriority(priorityValue);
        }

        // Time to live in milliseconds
        Long timeToLive = message.getIntValue(TIME_TO_LIVE_KEY);
        if (timeToLive != null) {
            long ttl = timeToLive;
            jcsmpMessage.setTimeToLive(ttl);
        }

        // Application message ID
        BString appMsgId = message.getStringValue(APPLICATION_MESSAGE_ID_KEY);
        if (appMsgId != null) {
            jcsmpMessage.setApplicationMessageId(appMsgId.getValue());
        }

        // Application message type
        BString appMsgType = message.getStringValue(APPLICATION_MESSAGE_TYPE_KEY);
        if (appMsgType != null) {
            jcsmpMessage.setApplicationMessageType(appMsgType.getValue());
        }

        // Correlation ID
        BString correlationId = message.getStringValue(CORRELATION_ID_KEY);
        if (correlationId != null) {
            jcsmpMessage.setCorrelationId(correlationId.getValue());
        }

        // Reply-to destination
        Object replyTo = message.get(REPLY_TO_KEY);
        if (replyTo instanceof BMap) {
            @SuppressWarnings("unchecked")
            BMap<BString, Object> replyToMap = (BMap<BString, Object>) replyTo;
            Destination replyToDestination = DestinationConverter.toJCSMPDestination(replyToMap);
            if (replyToDestination != null) {
                jcsmpMessage.setReplyTo(replyToDestination);
            }
        }

        // Sender ID
        BString senderId = message.getStringValue(SENDER_ID_KEY);
        if (senderId != null) {
            jcsmpMessage.setSenderId(senderId.getValue());
        }

        // Sender timestamp (milliseconds from epoch)
        Long senderTimestamp = message.getIntValue(SENDER_TIMESTAMP_KEY);
        if (senderTimestamp != null) {
            long timestamp = senderTimestamp;
            jcsmpMessage.setSenderTimestamp(timestamp);
        }

        // Sequence number
        Long sequenceNumber = message.getIntValue(SEQUENCE_NUMBER_KEY);
        if (sequenceNumber != null) {
            long seqNum = sequenceNumber;
            jcsmpMessage.setSequenceNumber(seqNum);
        }

        // User data (max 36 bytes)
        BArray userData = message.getArrayValue(USER_DATA_KEY);
        if (userData != null) {
            byte[] userDataBytes = userData.getBytes();
            if (userDataBytes.length > 36) {
                throw new IllegalArgumentException("userData cannot exceed 36 bytes, got " + userDataBytes.length);
            }
            jcsmpMessage.setUserData(userDataBytes);
        }

        // Custom properties (SDTMap)
        Object properties = message.get(PROPERTIES_KEY);
        if (properties instanceof BMap) {
            BMap<BString, Object> propsMap = (BMap<BString, Object>) properties;
            SDTMap sdtMap = PropertyConverter.ballerinaToSDTMap(propsMap);
            if (sdtMap != null && !sdtMap.isEmpty()) {
                jcsmpMessage.setProperties(sdtMap);
            }
        }
    }

    /**
     * Creates a BytesMessage from byte array payload. Uses attachment part instead of content part for the payload.
     */
    public static BytesMessage toByteMessage(XMLMessageProducer producer, byte[] content)
            throws Exception {
        if (producer == null) {
            throw new Exception("XMLMessageProducer cannot be null");
        }
        if (content == null) {
            throw new Exception("Message content cannot be null");
        }

        BytesMessage message = producer.createBytesMessage();
        if (message == null) {
            throw new Exception("Failed to create BytesXMLMessage");
        }

        // Use attachment instead of content
        message.setData(content);
        return message;
    }

}
