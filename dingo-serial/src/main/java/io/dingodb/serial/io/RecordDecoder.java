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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RecordDecoder {
    private final List<DingoSchema> schemas;
    private final short schemaVersion;

    private final byte unfishFlag;
    private final byte finishedFlag;
    private final byte deletedFlag;
    private byte[] transactionId = new byte[0];

    public RecordDecoder(List<DingoSchema> schemas, short schemaVersion,
                         byte unfishFlag, byte finishedFlag, byte deletedFlag, byte[] transactionId) {
        this(schemas, schemaVersion, unfishFlag, finishedFlag, deletedFlag, transactionId, false);
    }

    public RecordDecoder(List<DingoSchema> schemas, short schemaVersion,
                         byte unfishFlag, byte finishedFlag, byte deletedFlag, byte[] transactionId, boolean isKey) {
        if (!isKey) {
            Utils.sortSchema(schemas);
        }
        this.schemas = schemas;
        this.schemaVersion = schemaVersion;
        this.unfishFlag = unfishFlag;
        this.finishedFlag = finishedFlag;
        this.deletedFlag = deletedFlag;
        if (transactionId != null) {
            this.transactionId = transactionId;
        }
    }

    public Object[] decode(byte[] record) throws IOException {
        BinaryDecoder bd = new BinaryDecoder(record);
        bd.skipByte();
        bd.skipBytes();
        if (bd.readShort() == this.schemaVersion) {
            Object[] result = new Object[this.schemas.size()];
            for (DingoSchema schema : schemas) {
                switch (schema.getType()) {
                    case BOOLEAN:
                        result[schema.getIndex()] = bd.readBoolean();
                        break;
                    case BOOLEANLIST:
                        result[schema.getIndex()] = bd.readBooleanList();
                        break;
                    case SHORT:
                        result[schema.getIndex()] = bd.readShort();
                        break;
                    case SHORTLIST:
                        result[schema.getIndex()] = bd.readShortList();
                        break;
                    case INTEGER:
                        result[schema.getIndex()] = bd.readInt();
                        break;
                    case INTEGERLIST:
                        result[schema.getIndex()] = bd.readIntegerList();
                        break;
                    case FLOAT:
                        result[schema.getIndex()] = bd.readFloat();
                        break;
                    case FLOATLIST:
                        result[schema.getIndex()] = bd.readFloatList();
                        break;
                    case LONG:
                        result[schema.getIndex()] = bd.readLong();
                        break;
                    case LONGLIST:
                        result[schema.getIndex()] = bd.readLongList();
                        break;
                    case DOUBLE:
                        result[schema.getIndex()] = bd.readDouble();
                        break;
                    case DOUBLELIST:
                        result[schema.getIndex()] = bd.readDoubleList();
                        break;
                    case BYTES:
                        result[schema.getIndex()] = bd.readBytes();
                        break;
                    case BYTESLIST:
                        result[schema.getIndex()] = bd.readBytesList();
                        break;
                    case STRING:
                        result[schema.getIndex()] = bd.readString();
                        break;
                    case STRINGLIST:
                        result[schema.getIndex()] = bd.readStringList();
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
        bd.skipByte();
        bd.skipBytes();
        if (bd.readShort() == this.schemaVersion) {
            Object[] result = new Object[index.length];
            List<Integer> indexList
                = Arrays.stream(index).boxed().collect(Collectors.toList());
            for (DingoSchema schema : schemas) {
                int resultIndex = indexList.indexOf(schema.getIndex());
                if (resultIndex >= 0) {
                    switch (schema.getType()) {
                        case BOOLEAN:
                            result[resultIndex] = bd.readBoolean();
                            break;
                        case BOOLEANLIST:
                            result[resultIndex] = bd.readBooleanList();
                            break;
                        case SHORT:
                            result[resultIndex] = bd.readShort();
                            break;
                        case SHORTLIST:
                            result[resultIndex] = bd.readShortList();
                            break;
                        case INTEGER:
                            result[resultIndex] = bd.readInt();
                            break;
                        case INTEGERLIST:
                            result[resultIndex] = bd.readIntegerList();
                            break;
                        case FLOAT:
                            result[resultIndex] = bd.readFloat();
                            break;
                        case FLOATLIST:
                            result[resultIndex] = bd.readFloatList();
                            break;
                        case LONG:
                            result[resultIndex] = bd.readLong();
                            break;
                        case LONGLIST:
                            result[resultIndex] = bd.readLongList();
                            break;
                        case DOUBLE:
                            result[resultIndex] = bd.readDouble();
                            break;
                        case DOUBLELIST:
                            result[resultIndex] = bd.readDoubleList();
                            break;
                        case BYTES:
                            result[resultIndex] = bd.readBytes();
                            break;
                        case BYTESLIST:
                            result[resultIndex] = bd.readBytesList();
                            break;
                        case STRING:
                            result[resultIndex] = bd.readString();
                            break;
                        case STRINGLIST:
                            result[resultIndex] = bd.readStringList();
                            break;
                        default:
                            result[resultIndex] = null;
                    }
                } else {
                    switch (schema.getType()) {
                        case BOOLEANLIST:
                            bd.skipBooleanList();
                            break;
                        case SHORTLIST:
                            bd.skipShortList();
                            break;
                        case INTEGERLIST:
                            bd.skipIntegerList();
                            break;
                        case FLOATLIST:
                            bd.skipFloatList();
                            break;
                        case LONGLIST:
                            bd.skipLongList();
                            break;
                        case DOUBLELIST:
                            bd.skipDoubleList();
                            break;
                        case BYTES:
                            bd.skipBytes();
                            break;
                        case STRING:
                            bd.skipString();
                            break;
                        case BYTESLIST:
                            bd.skipBytesList();
                            break;
                        case STRINGLIST:
                            bd.skipStringList();
                            break;
                        default:
                            bd.skip(schema.getLength());
                    }
                }
            }
            return result;
        } else {
            throw new RuntimeException("Schema version Wrong!");
        }
    }

    public Object[] decodeKey(byte[] record) throws IOException {
        BinaryDecoder bd = new BinaryDecoder(record);
        bd.skipByte();
        bd.skipBytes();
        if (bd.readShort() == this.schemaVersion) {
            Object[] result = new Object[this.schemas.size()];
            for (DingoSchema schema : schemas) {
                switch (schema.getType()) {
                    case BOOLEAN:
                        result[schema.getIndex()] = bd.readBoolean();
                        break;
                    case BOOLEANLIST:
                        result[schema.getIndex()] = bd.readBooleanList();
                        break;
                    case SHORT:
                        result[schema.getIndex()] = bd.readKeyShort();
                        break;
                    case SHORTLIST:
                        result[schema.getIndex()] = bd.readShortList();
                        break;
                    case INTEGER:
                        result[schema.getIndex()] = bd.readKeyInt();
                        break;
                    case INTEGERLIST:
                        result[schema.getIndex()] = bd.readIntegerList();
                        break;
                    case FLOAT:
                        result[schema.getIndex()] = bd.readKeyFloat();
                        break;
                    case FLOATLIST:
                        result[schema.getIndex()] = bd.readFloatList();
                        break;
                    case LONG:
                        result[schema.getIndex()] = bd.readKeyLong();
                        break;
                    case LONGLIST:
                        result[schema.getIndex()] = bd.readLongList();
                        break;
                    case DOUBLE:
                        result[schema.getIndex()] = bd.readKeyDouble();
                        break;
                    case DOUBLELIST:
                        result[schema.getIndex()] = bd.readDoubleList();
                        break;
                    case BYTES:
                        result[schema.getIndex()] = bd.readKeyBytes();
                        break;
                    case BYTESLIST:
                        result[schema.getIndex()] = bd.readBytesList();
                        break;
                    case STRING:
                        result[schema.getIndex()] = bd.readKeyString();
                        break;
                    case STRINGLIST:
                        result[schema.getIndex()] = bd.readStringList();
                        break;
                    default:
                }
            }
            return result;
        }
        return null;
    }

    public Object[] decodeKey(byte[] record, int[] index) throws IOException {
        BinaryDecoder bd = new BinaryDecoder(record);
        bd.skipByte();
        bd.skipBytes();
        if (bd.readShort() == this.schemaVersion) {
            Object[] result = new Object[index.length];
            List<Integer> indexList
                = Arrays.stream(index).boxed().collect(Collectors.toList());
            for (DingoSchema schema : schemas) {
                int resultIndex = indexList.indexOf(schema.getIndex());
                if (resultIndex >= 0) {
                    switch (schema.getType()) {
                        case BOOLEAN:
                            result[resultIndex] = bd.readBoolean();
                            break;
                        case BOOLEANLIST:
                            result[resultIndex] = bd.readBooleanList();
                            break;
                        case SHORT:
                            result[resultIndex] = bd.readKeyShort();
                            break;
                        case SHORTLIST:
                            result[resultIndex] = bd.readShortList();
                            break;
                        case INTEGER:
                            result[resultIndex] = bd.readKeyInt();
                            break;
                        case INTEGERLIST:
                            result[resultIndex] = bd.readIntegerList();
                            break;
                        case FLOAT:
                            result[resultIndex] = bd.readKeyFloat();
                            break;
                        case FLOATLIST:
                            result[resultIndex] = bd.readFloatList();
                            break;
                        case LONG:
                            result[resultIndex] = bd.readKeyLong();
                            break;
                        case LONGLIST:
                            result[resultIndex] = bd.readLongList();
                            break;
                        case DOUBLE:
                            result[resultIndex] = bd.readKeyDouble();
                            break;
                        case DOUBLELIST:
                            result[resultIndex] = bd.readDoubleList();
                            break;
                        case BYTES:
                            result[resultIndex] = bd.readKeyBytes();
                            break;
                        case BYTESLIST:
                            result[resultIndex] = bd.readBytesList();
                            break;
                        case STRING:
                            result[resultIndex] = bd.readKeyString();
                            break;
                        case STRINGLIST:
                            result[resultIndex] = bd.readStringList();
                            break;
                        default:
                            result[resultIndex] = null;
                    }
                } else {
                    switch (schema.getType()) {
                        case BOOLEANLIST:
                            bd.skipBooleanList();
                            break;
                        case SHORTLIST:
                            bd.skipShortList();
                            break;
                        case INTEGERLIST:
                            bd.skipIntegerList();
                            break;
                        case FLOATLIST:
                            bd.skipFloatList();
                            break;
                        case LONGLIST:
                            bd.skipLongList();
                            break;
                        case DOUBLELIST:
                            bd.skipDoubleList();
                            break;
                        case BYTES:
                            bd.skipKeyBytes();
                            break;
                        case STRING:
                            bd.skipKeyString();
                            break;
                        case BYTESLIST:
                            bd.skipBytesList();
                            break;
                        case STRINGLIST:
                            bd.skipStringList();
                            break;
                        default:
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
