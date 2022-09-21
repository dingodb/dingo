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

package io.dingodb.common.operation;

import java.io.Serializable;
import java.util.Map;

public class DingoExecResult implements Serializable {
    private static final long serialVersionUID = -5528925734091175454L;

    // column=value/key={column=value}
    private Map<String, Value> record;
    private boolean isSuccess;
    private String errorMessage;
    private String op;

    public DingoExecResult(boolean isSuccess, String errorMessage) {
        this(null, isSuccess, errorMessage, null);
    }

    public DingoExecResult(Map<String, Value> record, boolean isSuccess, String errorMessage, String op) {
        this.record = record;
        this.isSuccess = isSuccess;
        this.errorMessage = errorMessage;
        this.op = op;
    }

    public Map<String, Value> getRecord() {
        return record;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public String errorMessage() {
        return errorMessage;
    }

    public String op() {
        return op;
    }

    /**
     * Get results by primary key and field name. Collection type operations only.
     *
     * <pre>
     *     record:{key1={column1=10,column2=20}, key2={column1=5,column2=15}}
     *     get(key1, column1).getObject() eq 10
     * </pre>
     *
     * @param key primary key
     * @param column column
     * @return result value
     */
    public Value get(String key, String column) {
        Value mapValue = record.get(key);
        if (mapValue.getType() == ParticleType.MAP) {
            Map map = (Map) mapValue.getObject();
            return Value.get(map.get(column));
        }
        return mapValue;
    }

    /**
     * Get results by field name. Numerical operations only.
     *
     * <pre>
     *     record:{column1=10,column2=20}
     *     get(column2) eq 20
     * </pre>
     * @param column column
     * @return result value
     */
    public Value get(String column) {
        return record.get(column.toUpperCase());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(500);
        sb.append('{');
        sb.append("record: ");
        if (record != null) {
            boolean sep = false;

            for (Map.Entry<String, Value> entry : record.entrySet()) {
                if (sep) {
                    sb.append(',');
                } else {
                    sep = true;
                }
                sb.append('{');
                sb.append(entry.getKey());
                sb.append(": ");
                if (entry.getValue().getType() == ParticleType.MAP) {
                    Map<String, Object> map = (Map<String, Object>) entry.getValue().getObject();
                    sb.append('{');
                    boolean b = false;
                    for (Map.Entry<String, Object> mapEntry : map.entrySet()) {
                        if (b) {
                            sb.append(',');
                        } else {
                            b = true;
                        }
                        sb.append(mapEntry.getKey());
                        sb.append(':');
                        sb.append(mapEntry.getValue());
                    }
                    sb.append('}');
                } else {
                    sb.append(':');
                    sb.append(entry.getValue().getObject());
                    sb.append('}');
                }
                if (sb.length() > 1000) {
                    sb.append("...");
                    break;
                }
            }
            sb.append('}');
        } else {
            sb.append("null");
        }
        sb.append(',');
        sb.append("isSuccess: ");
        sb.append(isSuccess);
        sb.append(", ");
        sb.append("errorMessage: ");
        sb.append('\"');
        sb.append(errorMessage);
        sb.append('\"');
        sb.append(", ");
        sb.append("op: ");
        if (op == null) {
            sb.append("null");
        } else {
            sb.append('\"');
            sb.append(op);
            sb.append('\"');
        }
        sb.append('}');

        return sb.toString();
    }
}
