/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.exec.converter;

import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.dingodb.common.mysql.constant.ServerConstant.ARRAY_SPLIT;

public class ImportFileConverter implements DataConverter {

    private final String NULL_FLG;
    public ImportFileConverter(byte[] escaped) {
        byte[] nullFlgBytes = new byte[escaped.length + 1];
        System.arraycopy(escaped, 0, nullFlgBytes, 0, escaped.length);
        nullFlgBytes[escaped.length] = "N".getBytes()[0];
        NULL_FLG = new String(nullFlgBytes);
    }

    @Override
    public Integer convertIntegerFrom(@NonNull Object value) {
        String valStr = (String) value;
        if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            return Integer.parseInt(valStr);
        }
    }

    @Override
    public Long convertLongFrom(@NonNull Object value) {
        String valStr = (String) value;
        if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            return Long.parseLong(valStr);
        }
    }

    @Override
    public Float convertFloatFrom(@NonNull Object value) {
        String valStr = (String) value;
        if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            return Float.parseFloat(valStr);
        }
    }

    @Override
    public Double convertDoubleFrom(@NonNull Object value) {
        String valStr = (String) value;
        if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            return Double.parseDouble(valStr);
        }
    }

    @Override
    public Boolean convertBooleanFrom(@NonNull Object value) {
        String valStr = value.toString();
        if ("1".equals(valStr)) {
            return true;
        } else if ("0".equals(valStr)) {
            return false;
        } else if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            return null;
        }
    }

    @Override
    public BigDecimal convertDecimalFrom(@NonNull Object value) {
        String valStr = (String) value;
        if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            return new BigDecimal(valStr);
        }
    }

    @Override
    public Date convertDateFrom(@NonNull Object value) {
        String valStr = (String) value;
        if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            return DateTimeUtils.parseDate(valStr);
        }
    }

    @Override
    public Time convertTimeFrom(@NonNull Object value) {
        String valStr = (String) value;
        if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            return DateTimeUtils.parseTime(valStr);
        }
    }

    @Override
    public Timestamp convertTimestampFrom(@NonNull Object value) {
        String valStr = (String) value;
        if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            return DateTimeUtils.parseTimestamp(valStr);
        }
    }

    @Override
    public String convertStringFrom(@NonNull Object value) {
        String valStr = (String) value;
        if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            return valStr;
        }
    }

    @Override
    public Object[] convertArrayFrom(@NonNull Object value, @NonNull DingoType elementType) {
        String valStr = (String) value;
        if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            return splitArray(value.toString().getBytes(), (byte) ARRAY_SPLIT);
        }
    }

    @Override
    public List<?> convertListFrom(@NonNull Object value, @NonNull DingoType elementType) {
        String valStr = (String) value;
        if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            valStr = valStr.substring(1, valStr.length() - 1);
            Object[] tuples = splitArray(valStr.getBytes(), (byte) ARRAY_SPLIT);
            List<Object> res = new ArrayList<>();
            for (Object obj : tuples) {
                 res.add(elementType.convertFrom(obj, this));
            }
            return res;
        }
    }

    @Override
    public Map<Object, Object> convertMapFrom(@NonNull Object value, @NonNull DingoType keyType, @NonNull DingoType valueType) {
        String valStr = (String) value;
        if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            return mapStringToMap(valStr);
        }
    }

    @Override
    public byte[] convertBinaryFrom(@NonNull Object value) {
        String valStr = value.toString();
        if (NULL_FLG.equalsIgnoreCase(valStr)) {
            return null;
        } else {
            return Base64.getDecoder().decode(valStr);
        }
    }

    public static Map<Object, Object> mapStringToMap(String str){
        str = str.substring(1, str.length()-1);
        String[] strs = str.split(",");
        Map<Object, Object> map = new HashMap<>();
        for (String string : strs) {
            String key = string.split("=")[0];
            String value = string.split("=")[1];
            String key1 = key.trim();
            String value1 = value.trim();
            map.put(key1, value1);
        }
        return map;
    }

    private static Object[] splitArray(byte[] bytes, byte terminated) {
        int len = bytes.length;
        int fieldBreakPos = 0;
        List<String> tupleList = new ArrayList<>();
        for (int i = 0; i < len; i ++) {
            byte b = bytes[i];
            if (b == terminated) {
                byte[] fieldBytes = new byte[i - fieldBreakPos];
                System.arraycopy(bytes, fieldBreakPos, fieldBytes, 0, fieldBytes.length);
                String valTmp = new String(fieldBytes);
                if ("\\N".equalsIgnoreCase(valTmp)) {
                    tupleList.add(valTmp);
                } else {
                    tupleList.add(valTmp);
                }
                fieldBreakPos = i + 1;
            }
        }
        if (fieldBreakPos <= len - 1) {
            byte[] fieldBytes = new byte[len - fieldBreakPos];
            System.arraycopy(bytes, fieldBreakPos, fieldBytes, 0, fieldBytes.length);
            String valTmp = new String(fieldBytes);
            if ("\\N".equalsIgnoreCase(valTmp)) {
                tupleList.add(valTmp);
            } else {
                tupleList.add(valTmp);
            }
        }

        return tupleList.toArray(new String[0]);
    }
}
