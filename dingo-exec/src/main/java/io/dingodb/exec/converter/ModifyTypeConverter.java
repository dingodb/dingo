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

import io.dingodb.common.mysql.DingoErrUtil;
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

import static io.dingodb.common.mysql.error.ErrorCode.ErrTruncatedWrongValue;

public class ModifyTypeConverter implements DataConverter {

    @Override
    public Integer convertIntegerFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            try {
                return new BigDecimal(valStr).intValue();
            } catch (Exception e) {
                throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "int", valStr);
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
                try {
                    return new BigDecimal(valStr).longValue();
                } catch (Exception e1) {
                    throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "long", valStr);
                }
            }
        }
    }

    @Override
    public Float convertFloatFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            try {
                return new BigDecimal(valStr).floatValue();
            } catch (Exception e) {
                throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "float", valStr);
            }
        }
    }

    @Override
    public Double convertDoubleFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            try {
                return new BigDecimal(valStr).doubleValue();
            } catch (Exception e) {
                throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "double", valStr);
            }
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
            try {
                return new BigDecimal(valStr);
            } catch (Exception e) {
                throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "decimal", valStr);
            }
        }
    }

    @Override
    public Date convertDateFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            try {
                return DateTimeUtils.parseDate(valStr);
            } catch (Exception e) {
                throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "date", valStr);
            }
        }
    }

    @Override
    public Time convertTimeFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            try {
                return DateTimeUtils.parseTime(valStr);
            } catch (Exception e) {
                throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "time", valStr);
            }
        }
    }

    @Override
    public Timestamp convertTimestampFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            try {
                return DateTimeUtils.parseTimestamp(valStr);
            } catch (Exception e) {
                throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "timestamp", valStr);
            }
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
            throw new RuntimeException("not supported");
        }
    }

    @Override
    public List<?> convertListFrom(@NonNull Object value, @NonNull DingoType elementType) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            throw new RuntimeException("not supported");
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
            throw new RuntimeException("not supported");
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
