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

import io.dingodb.serial.schema.DingoSchema;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;

import java.util.List;

public class LuaJUtils {

    public static LuaTable getLuaTable(List<DingoSchema> schemaList, Object[] record) {
        LuaTable table = new LuaTable();
        for (DingoSchema schema : schemaList) {
            switch (schema.getType()) {
                case BOOLEAN:
                    table.set(schema.getIndex(), LuaValue.valueOf((boolean) record[schema.getIndex()]));
                    break;
                case SHORT:
                    table.set(schema.getIndex(), LuaValue.valueOf((short) record[schema.getIndex()]));
                    break;
                case INTEGER:
                    table.set(schema.getIndex(), LuaValue.valueOf((int) record[schema.getIndex()]));
                    break;
                case FLOAT:
                    table.set(schema.getIndex(), LuaValue.valueOf((float) record[schema.getIndex()]));
                    break;
                case LONG:
                    table.set(schema.getIndex(), LuaValue.valueOf((long) record[schema.getIndex()]));
                    break;
                case DOUBLE:
                    table.set(schema.getIndex(), LuaValue.valueOf((double) record[schema.getIndex()]));
                    break;
                case STRING:
                    table.set(schema.getIndex(), LuaValue.valueOf((String) record[schema.getIndex()]));
                    break;
                case BYTES:
                    table.set(schema.getIndex(), LuaValue.valueOf((byte[]) record[schema.getIndex()]));
                    break;
                default:
            }
        }
        return table;
    }

    public static Object[] getObject(List<DingoSchema> schemaList, LuaValue result) {
        Object[] record = new Object[schemaList.size()];
        for (DingoSchema schema : schemaList) {
            switch (schema.getType()) {
                case BOOLEAN:
                    record[schema.getIndex()] = result.get(schema.getIndex()).toboolean();
                    break;
                case SHORT:
                    record[schema.getIndex()] = result.get(schema.getIndex()).toshort();
                    break;
                case INTEGER:
                    record[schema.getIndex()] = result.get(schema.getIndex()).toint();
                    break;
                case FLOAT:
                    record[schema.getIndex()] = result.get(schema.getIndex()).tofloat();
                    break;
                case LONG:
                    record[schema.getIndex()] = result.get(schema.getIndex()).tolong();
                    break;
                case DOUBLE:
                    record[schema.getIndex()] = result.get(schema.getIndex()).todouble();
                    break;
                case STRING:
                    record[schema.getIndex()] = result.get(schema.getIndex()).toString();
                    break;
                case BYTES:
                    //record[schema.getIndex()] = result.get(schema.getIndex()).tobyte();
                    //TODO byte[]
                    break;
                default:
            }
        }
        return record;
    }

}
