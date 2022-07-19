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

package io.dingodb.serial.io;

import io.dingodb.serial.schema.DingoSchema;
import io.dingodb.serial.util.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class RecordDecoder {
    private List<DingoSchema> schemas;
    private short schemaVersion;

    public RecordDecoder(List<DingoSchema> schemas, short schemaVersion) {
        Utils.sortSchema(schemas);
        this.schemas = schemas;
        this.schemaVersion = schemaVersion;
    }

    public Object[] decode(byte[] record) throws IOException {
        BinaryDecoder bd = new BinaryDecoder(record);
        if (bd.readShort() == this.schemaVersion) {
            Object[] result = new Object[this.schemas.size()];
            for (DingoSchema schema : schemas) {
                switch (schema.getType()) {
                    case BOOLEAN:
                        result[schema.getIndex()] = bd.readBoolean();
                        break;
                    case SHORT:
                        result[schema.getIndex()] = bd.readShort();
                        break;
                    case INTEGER:
                        result[schema.getIndex()] = bd.readInt();
                        break;
                    case FLOAT:
                        result[schema.getIndex()] = bd.readFloat();
                        break;
                    case LONG:
                        result[schema.getIndex()] = bd.readLong();
                        break;
                    case DOUBLE:
                        result[schema.getIndex()] = bd.readDouble();
                        break;
                    case STRING:
                        result[schema.getIndex()] = bd.readString();
                        break;
                    default:
                }
            }
            return result;
        }
        return null;
    }

    public Object[] decode(byte[] record, int[] index) throws IOException {
        BinaryDecoder bd = new BinaryDecoder(record);
        if (bd.readShort() == this.schemaVersion) {
            Object result[] = new Object[index.length];
            List<Integer> indexList
                = Arrays.stream(index).boxed().collect(Collectors.toList());
            for (DingoSchema schema : schemas) {
                int resultIndex = indexList.indexOf(schema.getIndex());
                if (resultIndex >= 0) {
                    switch (schema.getType()) {
                        case BOOLEAN:
                            result[resultIndex] = bd.readBoolean();
                            break;
                        case SHORT:
                            result[resultIndex] = bd.readShort();
                            break;
                        case INTEGER:
                            result[resultIndex] = bd.readInt();
                            break;
                        case FLOAT:
                            result[resultIndex] = bd.readFloat();
                            break;
                        case LONG:
                            result[resultIndex] = bd.readLong();
                            break;
                        case DOUBLE:
                            result[resultIndex] = bd.readDouble();
                            break;
                        case STRING:
                            result[resultIndex] = bd.readString();
                            break;
                        default:
                            result[resultIndex] = null;
                    }
                } else {
                    if (Utils.lengthNotSure(schema)) {
                        if (!bd.readIsNull()) {
                            bd.skip(bd.readIntNoNull());
                        }
                    } else {
                        bd.skip(schema.getLength());
                    }
                }
            }

            return result;
        } else {
            throw new RuntimeException("Schema version Wrong!");
        }
    }
}
