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

package io.dingodb.common.util;


import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;

@Slf4j
public final class Datum {
    private Datum() {
    }

    public static Object convertCalcite(Object value) {
        RexLiteral literal = (RexLiteral) value;
        RelDataType type = literal.getType();
        Object retValue  = literal.getValue();

        switch (type.getSqlTypeName().getName()) {
            case "INTEGER":
            case "LONG": {
                retValue = Long.valueOf(literal.getValue().toString());
                break;
            }
            case "DECIMAL":
            case "DOUBLE": {
                int result = ((BigDecimal)(literal.getValue())).compareTo(BigDecimal.valueOf(1000));
                if (result < 0) {
                    retValue = Double.valueOf(literal.getValue().toString());
                } else {
                    retValue = literal.getValue();
                }
                break;
            }
            case "CHAR":
            case "VARCHAR": {
                if (literal.getValue() instanceof NlsString) {
                    retValue = ((NlsString) literal.getValue()).getValue();
                }
                break;
            }
            case "TIMESTAMP":
            case "TIME":
            case "DATE": {
                retValue = ((java.util.GregorianCalendar) (literal.getValue())).getTimeInMillis();
                break;
            }
            default: {
                break;
            }
        }
        return retValue;
    }

}
