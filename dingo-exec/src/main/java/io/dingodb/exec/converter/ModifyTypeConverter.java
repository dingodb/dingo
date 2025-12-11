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
import io.dingodb.common.time.DingoTimeZoneContext;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.common.util.Utils;
import io.dingodb.expr.common.timezone.core.DateTimeType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import static io.dingodb.common.mysql.error.ErrorCode.ErrDataOutOfRange;
import static io.dingodb.common.mysql.error.ErrorCode.ErrTruncatedWrongValue;
import static io.dingodb.common.mysql.error.ErrorCode.ErrWarnDataOutOfRange;

public class ModifyTypeConverter implements DataConverter {

    @Override
    public Integer convertIntegerFrom(@NonNull Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Long) {
            Long val = (Long) value;
            if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                return val.intValue();
            } else {
                throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "int", val);
            }
        }

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
    public Integer convertTinyIntFrom(@NonNull Object value) {
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            try {
                int val = new BigDecimal(valStr).intValue();
                if (Utils.tinyintInRange(val)) {
                    return val;
                } else {
                    throw DingoErrUtil.newStdErr(ErrWarnDataOutOfRange, "1");
                }
            } catch (Exception e) {
                throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "tinyint", valStr);
            }
        }
    }

    @Override
    public Long convertLongFrom(@NonNull Object value) {
        if (value instanceof Long) {
            return (Long) value;
        }
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
        if (value instanceof Float) {
            return (Float) value;
        }
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
        if (value instanceof Double) {
            return (Double) value;
        }
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
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        String valStr = strVal(value);
        if ("1".equals(valStr)) {
            return true;
        } else if ("0".equals(valStr)) {
            return false;
        } else {
            throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "boolean", valStr);
        }
    }

    @Override
    public BigDecimal convertDecimalFrom(@NonNull Object value, int precision, int scale) {
        BigDecimal valDecimal = convertDecimalFrom(value);
        valDecimal = valDecimal.setScale(scale, RoundingMode.HALF_UP);
        if (valDecimal.precision() > precision) {
            String formatErr = "DECIMAL value is out of range in '(%d, %d)'";
            formatErr = formatErr.formatted(precision, scale);
            throw DingoErrUtil.newStdErr(formatErr, ErrDataOutOfRange);
        }
        return valDecimal;
    }

    @Override
    public BigDecimal convertDecimalFrom(@NonNull Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
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
        if (value instanceof Date) {
            return (Date) value;
        }
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            try {
                Date date = (Date) DingoTimeZoneContext.getProcessor().processDateTime(valStr, DateTimeType.DATE);
                if (date == null) {
                    if (value instanceof Time) {
                        return new Date(System.currentTimeMillis());
                    }
                    throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "date", valStr);
                }
                return date;
            } catch (Exception e) {
                throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "date", valStr);
            }
        }
    }

    @Override
    public Time convertTimeFrom(@NonNull Object value) {
        if (value instanceof Time) {
            return (Time) value;
        }
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            try {
                return (Time) DingoTimeZoneContext.getProcessor().processDateTime(valStr, DateTimeType.TIME);
            } catch (Exception e) {
                throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "time", valStr);
            }
        }
    }

    @Override
    public Timestamp convertTimestampFrom(@NonNull Object value) {
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        String valStr = strVal(value);
        if (valStr == null) {
            return null;
        } else {
            try {
                Timestamp timestamp =
                    (Timestamp) DingoTimeZoneContext.getProcessor().processDateTime(valStr, DateTimeType.TIMESTAMP);
                if (timestamp == null) {
                    throw DingoErrUtil.newStdErr(ErrTruncatedWrongValue, "timestamp", valStr);
                }
                return timestamp;
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
