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

import java.util.ArrayList;
import java.util.List;

public final class UdfUtils {

    private UdfUtils() {
    }

    public static LuaTable getLuaTable(List<DingoSchema> schemaList, Object[] record) {
        LuaTable table = new LuaTable();
        for (DingoSchema schema : schemaList) {
            switch (schema.getType()) {
                case BOOLEAN:
                    table.set(schema.getIndex(), LuaValue.valueOf((boolean) record[schema.getIndex()]));
                    break;
                case BOOLEANLIST:
                    LuaTable booleanList = new LuaTable();
                    List<Boolean> booleans = (List<Boolean>) record[schema.getIndex()];
                    for (int i = 0; i < booleans.size(); i++) {
                        booleanList.set(i, LuaValue.valueOf(booleans.get(i)));
                    }
                    table.set(schema.getIndex(), booleanList);
                    break;
                case SHORT:
                    table.set(schema.getIndex(), LuaValue.valueOf((short) record[schema.getIndex()]));
                    break;
                case SHORTLIST:
                    LuaTable shortList = new LuaTable();
                    List<Short> shorts = (List<Short>) record[schema.getIndex()];
                    for (int i = 0; i < shorts.size(); i++) {
                        shortList.set(i, LuaValue.valueOf(shorts.get(i)));
                    }
                    table.set(schema.getIndex(), shortList);
                    break;
                case INTEGER:
                    table.set(schema.getIndex(), LuaValue.valueOf((int) record[schema.getIndex()]));
                    break;
                case INTEGERLIST:
                    LuaTable intList = new LuaTable();
                    List<Integer> ints = (List<Integer>) record[schema.getIndex()];
                    for (int i = 0; i < ints.size(); i++) {
                        intList.set(i, LuaValue.valueOf(ints.get(i)));
                    }
                    table.set(schema.getIndex(), intList);
                    break;
                case FLOAT:
                    table.set(schema.getIndex(), LuaValue.valueOf((float) record[schema.getIndex()]));
                    break;
                case FLOATLIST:
                    LuaTable floatList = new LuaTable();
                    List<Float> floats = (List<Float>) record[schema.getIndex()];
                    for (int i = 0; i < floats.size(); i++) {
                        floatList.set(i, LuaValue.valueOf(floats.get(i)));
                    }
                    table.set(schema.getIndex(), floatList);
                    break;
                case LONG:
                    table.set(schema.getIndex(), LuaValue.valueOf((long) record[schema.getIndex()]));
                    break;
                case LONGLIST:
                    LuaTable longList = new LuaTable();
                    List<Long> longs = (List<Long>) record[schema.getIndex()];
                    for (int i = 0; i < longs.size(); i++) {
                        longList.set(i, LuaValue.valueOf(longs.get(i)));
                    }
                    table.set(schema.getIndex(), longList);
                    break;
                case DOUBLE:
                    table.set(schema.getIndex(), LuaValue.valueOf((double) record[schema.getIndex()]));
                    break;
                case DOUBLELIST:
                    LuaTable doubleList = new LuaTable();
                    List<Double> doubles = (List<Double>) record[schema.getIndex()];
                    for (int i = 0; i < doubles.size(); i++) {
                        doubleList.set(i, LuaValue.valueOf(doubles.get(i)));
                    }
                    table.set(schema.getIndex(), doubleList);
                    break;
                case STRING:
                    table.set(schema.getIndex(), LuaValue.valueOf((String) record[schema.getIndex()]));
                    break;
                case STRINGLIST:
                    LuaTable stringList = new LuaTable();
                    List<String> strings = (List<String>) record[schema.getIndex()];
                    for (int i = 0; i < strings.size(); i++) {
                        stringList.set(i, LuaValue.valueOf(strings.get(i)));
                    }
                    table.set(schema.getIndex(), stringList);
                    break;
                case BYTES:
                    table.set(schema.getIndex(), LuaValue.valueOf((byte[]) record[schema.getIndex()]));
                    break;
                case BYTESLIST:
                    LuaTable bytesList = new LuaTable();
                    List<byte[]> bytess = (List<byte[]>) record[schema.getIndex()];
                    for (int i = 0; i < bytess.size(); i++) {
                        bytesList.set(i, LuaValue.valueOf(bytess.get(i)));
                    }
                    table.set(schema.getIndex(), bytesList);
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
                case BOOLEANLIST:
                    LuaTable booleanList = (LuaTable) result.get(schema.getIndex());
                    List<Boolean> booleans = new ArrayList<>();
                    for (int i = 0; i <= booleanList.length(); i++) {
                        booleans.add(i, booleanList.get(i).toboolean());
                    }
                    record[schema.getIndex()] = booleans;
                    break;
                case SHORT:
                    record[schema.getIndex()] = result.get(schema.getIndex()).toshort();
                    break;
                case SHORTLIST:
                    LuaTable shortList = (LuaTable) result.get(schema.getIndex());
                    List<Short> shorts = new ArrayList<>();
                    for (int i = 0; i <= shortList.length(); i++) {
                        shorts.add(i, shortList.get(i).toshort());
                    }
                    record[schema.getIndex()] = shorts;
                    break;
                case INTEGER:
                    record[schema.getIndex()] = result.get(schema.getIndex()).toint();
                    break;
                case INTEGERLIST:
                    LuaTable intList = (LuaTable) result.get(schema.getIndex());
                    List<Integer> ints = new ArrayList<>();
                    for (int i = 0; i <= intList.length(); i++) {
                        ints.add(i, intList.get(i).toint());
                    }
                    record[schema.getIndex()] = ints;
                    break;
                case FLOAT:
                    record[schema.getIndex()] = result.get(schema.getIndex()).tofloat();
                    break;
                case FLOATLIST:
                    LuaTable floatList = (LuaTable) result.get(schema.getIndex());
                    List<Float> floats = new ArrayList<>();
                    for (int i = 0; i <= floatList.length(); i++) {
                        floats.add(i, floatList.get(i).tofloat());
                    }
                    record[schema.getIndex()] = floats;
                    break;
                case LONG:
                    record[schema.getIndex()] = result.get(schema.getIndex()).tolong();
                    break;
                case LONGLIST:
                    LuaTable longList = (LuaTable) result.get(schema.getIndex());
                    List<Long> longs = new ArrayList<>();
                    for (int i = 0; i <= longList.length(); i++) {
                        longs.add(i, longList.get(i).tolong());
                    }
                    record[schema.getIndex()] = longs;
                    break;
                case DOUBLE:
                    record[schema.getIndex()] = result.get(schema.getIndex()).todouble();
                    break;
                case DOUBLELIST:
                    LuaTable doubleList = (LuaTable) result.get(schema.getIndex());
                    List<Double> doubles = new ArrayList<>();
                    for (int i = 0; i <= doubleList.length(); i++) {
                        doubles.add(i, doubleList.get(i).todouble());
                    }
                    record[schema.getIndex()] = doubles;
                    break;
                case STRING:
                    record[schema.getIndex()] = result.get(schema.getIndex()).toString();
                    break;
                case STRINGLIST:
                    LuaTable stringList = (LuaTable) result.get(schema.getIndex());
                    List<String> strings = new ArrayList<>();
                    for (int i = 0; i <= stringList.length(); i++) {
                        strings.add(i, stringList.get(i).toString());
                    }
                    record[schema.getIndex()] = strings;
                    break;
                case BYTES:
                    //record[schema.getIndex()] = result.get(schema.getIndex()).tobyte();
                    //TODO byte[]
                    break;
                case BYTESLIST:
                    //LuaTable bytesList = (LuaTable) result.get(schema.getIndex());
                    //List<byte[]> bytess = new ArrayList<>();
                    //for (int i = 0; i <= bytesList.length(); i++) {
                    //    bytess.add(i, bytesList.get(i).tobyte());
                    //}
                    //record[schema.getIndex()] = bytess;
                    //TODO byte[]list
                    break;
                default:
            }
        }
        return record;
    }
}
