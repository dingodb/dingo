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
import java.util.List;
import java.util.Map;

public class ModifyTypeConverter implements DataConverter {

    @Override
    public Integer convertIntegerFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            try {
                return Integer.parseInt(valStr);
            } catch (Exception e) {
                return new BigDecimal(valStr).intValue();
            }
        }
    }

    @Override
    public Long convertLongFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            try {
                return Long.parseLong(valStr);
            } catch (Exception e) {
                return new BigDecimal(valStr).longValue();
            }
        }
    }

    @Override
    public Float convertFloatFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            return Float.parseFloat(valStr);
        }
    }

    @Override
    public Double convertDoubleFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            return Double.parseDouble(valStr);
        }
    }

    @Override
    public Boolean convertBooleanFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if ("1".equals(valStr)) {
            return true;
        } else if ("0".equals(valStr)) {
            return false;
        } else {
            return null;
        }
    }

    @Override
    public BigDecimal convertDecimalFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            return new BigDecimal(valStr);
        }
    }

    @Override
    public Date convertDateFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            return DateTimeUtils.parseDate(valStr);
        }
    }

    @Override
    public Time convertTimeFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            return DateTimeUtils.parseTime(valStr);
        }
    }

    @Override
    public Timestamp convertTimestampFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            return DateTimeUtils.parseTimestamp(valStr);
        }
    }

    @Override
    public String convertStringFrom(@NonNull Object value) {
        return strVal(value);
    }

    @Override
    public Object[] convertArrayFrom(@NonNull Object value, @NonNull DingoType elementType) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            throw new RuntimeException("xx");
        }
    }

    @Override
    public List<?> convertListFrom(@NonNull Object value, @NonNull DingoType elementType) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            throw new RuntimeException("xx");
        }
    }

    @Override
    public Map<Object, Object> convertMapFrom(
        @NonNull Object value, @NonNull DingoType keyType, @NonNull DingoType valueType
    ) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            throw new RuntimeException("xx");
        }
    }

    @Override
    public byte[] convertBinaryFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            return valStr.getBytes();
        }
    }

    public String strVal(Object str) {
        if (str == null) {
            return null;
        }
        return str.toString();
    }

}
