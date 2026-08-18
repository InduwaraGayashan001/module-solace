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

package io.xlibb.solace.common;

import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;


/**
 * Utility for bidirectional conversion between JCSMP SDTMap and Ballerina map.
 */
public class PropertyConverter {

    /**
     * Converts a JCSMP SDTMap to a JSON string.
     *
     * @param sdtMap the JCSMP SDTMap
     * @return JSON string representation
     * @throws SDTException if SDT conversion fails
     */
    public static String sdtMapToJson(SDTMap sdtMap) throws SDTException {
        Map<String, Object> map = new HashMap<>();
        for (String key : sdtMap.keySet()) {
            Object value = sdtMap.get(key);
            map.put(key, convertSDTValueToJsonCompatible(value));
        }
        return new JSONObject(map).toString();
    }

    private static Object convertSDTValueToJsonCompatible(Object value) throws SDTException {
        if (value instanceof SDTMap nestedMap) {
            Map<String, Object> nestedJsonMap = new HashMap<>();
            for (String key : nestedMap.keySet()) {
                nestedJsonMap.put(key, convertSDTValueToJsonCompatible(nestedMap.get(key)));
            }
            return nestedJsonMap;
        } else if (value instanceof byte[] bytes) {
            return java.util.Base64.getEncoder().encodeToString(bytes);
        }
        return value;
    }

    /**
     * Converts a JCSMP SDTMap to a Ballerina map.
     *
     * @param sdtMap        the JCSMP SDTMap
     * @param ballerinaType the Ballerina MapType for type safety (null for untyped maps)
     * @return Ballerina map with converted values
     * @throws SDTException if SDT conversion fails
     */
    public static BMap<BString, Object> sdtMapToBallerina(SDTMap sdtMap, MapType ballerinaType)
            throws SDTException {
        // Create typed map if MapType provided, otherwise create untyped map
        BMap<BString, Object> messageProperties = (ballerinaType != null)
                ? ValueCreator.createMapValue(ballerinaType)
                : ValueCreator.createMapValue();

        for (String key : sdtMap.keySet()) {
            Object value = sdtMap.get(key);

            if (value == null) {
                continue;
            }

            // Convert SDT value to Ballerina value
            Object ballerinaValue = convertSDTValueToBallerina(value);
            if (ballerinaValue != null) {
                messageProperties.put(StringUtils.fromString(key), ballerinaValue);
            }
        }

        return messageProperties;
    }

    /**
     * Converts a Ballerina map to a JCSMP SDTMap.
     *
     * @param propsMap the Ballerina properties map
     * @return JCSMP SDTMap, or null if input is null/empty
     * @throws SDTException if SDT conversion fails
     */
    public static SDTMap ballerinaToSDTMap(BMap<BString, Object> propsMap) throws SDTException {
        if (propsMap == null || propsMap.isEmpty()) {
            return null;
        }

        SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();

        for (BString key : propsMap.getKeys()) {
            Object value = propsMap.get(key);
            String keyStr = key.getValue();

            if (value == null) {
                continue;
            }

            // Convert Ballerina value to SDT value
            convertBallerinaValueToSDT(sdtMap, keyStr, value);
        }

        return sdtMap;
    }

    private static Object convertSDTValueToBallerina(Object value) throws SDTException {
        return switch (value) {
            case null -> null;
            case String s -> StringUtils.fromString(s);
            case Boolean b -> b;
            case Integer i -> i.longValue();
            case Long l -> l;
            case Float v -> v.doubleValue();
            case Double v -> v;
            case Byte b -> b.longValue();
            case Short i -> i.longValue();
            case byte[] bytes -> ValueCreator.createArrayValue(bytes);
            case SDTMap nestedMap -> sdtMapToBallerina(nestedMap, null);
            default -> StringUtils.fromString(value.toString());
        };

    }

    private static void convertBallerinaValueToSDT(SDTMap sdtMap, String key, Object value)
            throws SDTException {
        switch (value) {
            case null -> {
            }
            case BString bString -> sdtMap.putString(key, bString.getValue());
            case Boolean b -> sdtMap.putBoolean(key, b);
            case Long l -> sdtMap.putLong(key, l);
            case Integer i -> sdtMap.putInteger(key, i);
            case Double v -> sdtMap.putDouble(key, v);
            case Float v -> sdtMap.putFloat(key, v);
            case BArray bArray -> sdtMap.putBytes(key, bArray.getBytes());
            default -> sdtMap.putString(key, value.toString());
        }
    }

}
