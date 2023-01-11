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

public class RecordEncoder {
    private final List<DingoSchema> schemas;
    private final short schemaVersion;
    private final int approPerRecordSize;
    private final int perRecordKeySize;

    private final byte unfishFlag;
    private final byte finishedFlag;
    private final byte deletedFlag;
    private byte[] transactionId = new byte[0];


    public RecordEncoder(List<DingoSchema> schemas, short schemaVersion,
                         byte unfishFlag, byte finishedFlag, byte deletedFlag, byte[] transactionId) {
        this(schemas, schemaVersion, unfishFlag, finishedFlag, deletedFlag, transactionId, false);
    }

    public RecordEncoder(List<DingoSchema> schemas, short schemaVersion,
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
        int[] approSize = Utils.getApproPerRecordSize(schemas);
        this.approPerRecordSize = approSize[0] + 9 + this.transactionId.length;
        this.perRecordKeySize = approSize[1];
    }

    public byte[] encode(Object[] record) throws IOException {
        BinaryEncoder be = new BinaryEncoder(new byte[approPerRecordSize]);
        be.write(finishedFlag);
        be.writeBytes(transactionId);
        be.writeShort(schemaVersion);
        for (DingoSchema schema : schemas) {
            switch (schema.getType()) {
                case BOOLEAN:
                    be.writeBoolean(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case BOOLEANLIST:
                    be.writeBooleanList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case SHORT:
                    be.writeShort(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case SHORTLIST:
                    be.writeShortList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case INTEGER:
                    be.writeInt(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case INTEGERLIST:
                    be.writeIntegerList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case FLOAT:
                    be.writeFloat(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case FLOATLIST:
                    be.writeFloatList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case LONG:
                    be.writeLong(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case LONGLIST:
                    be.writeLongList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case DOUBLE:
                    be.writeDouble(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case DOUBLELIST:
                    be.writeDoubleList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case BYTES:
                    be.writeBytes(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case BYTESLIST:
                    be.writeBytesList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case STRING:
                    be.writeString(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case STRINGLIST:
                    be.writeStringList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                default:
            }
        }
        return be.getByteArray();
    }

    public byte[] encode(byte[] record, int[] index, Object[] columns) throws IOException {
        BinaryEncoder be = new BinaryEncoder(record);
        be.skipByte();
        be.skipBytes();
        if (be.readShort() == this.schemaVersion) {
            List<Integer> indexList
                = Arrays.stream(index).boxed().collect(Collectors.toList());
            for (DingoSchema schema : schemas) {
                int columnIndex = indexList.indexOf(schema.getIndex());
                if (columnIndex >= 0) {
                    switch (schema.getType()) {
                        case BOOLEAN:
                            be.writeBoolean(Utils.processNullColumn(schema, columns[columnIndex]));
                            break;
                        case BOOLEANLIST:
                            be.updateBooleanList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case SHORT:
                            be.writeShort(Utils.processNullColumn(schema, columns[columnIndex]));
                            break;
                        case SHORTLIST:
                            be.updateShortList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case INTEGER:
                            be.writeInt(Utils.processNullColumn(schema, columns[columnIndex]));
                            break;
                        case INTEGERLIST:
                            be.updateIntegerList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case FLOAT:
                            be.writeFloat(Utils.processNullColumn(schema, columns[columnIndex]));
                            break;
                        case FLOATLIST:
                            be.updateFloatList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case LONG:
                            be.writeLong(Utils.processNullColumn(schema, columns[columnIndex]));
                            break;
                        case LONGLIST:
                            be.updateLongList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case DOUBLE:
                            be.writeDouble(Utils.processNullColumn(schema, columns[columnIndex]));
                            break;
                        case DOUBLELIST:
                            be.updateDoubleList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case STRING:
                            be.updateString(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case STRINGLIST:
                            be.updateStringList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case BYTES:
                            be.updateBytes(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case BYTESLIST:
                            be.updateBytesList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        default:
                    }
                } else {
                    switch (schema.getType()) {
                        case BOOLEANLIST:
                            be.skipBooleanList();
                            break;
                        case SHORTLIST:
                            be.skipShortList();
                            break;
                        case INTEGERLIST:
                            be.skipIntegerList();
                            break;
                        case FLOATLIST:
                            be.skipFloatList();
                            break;
                        case LONGLIST:
                            be.skipLongList();
                            break;
                        case DOUBLELIST:
                            be.skipDoubleList();
                            break;
                        case STRING:
                        case BYTES:
                            be.skipString();
                            break;
                        case STRINGLIST:
                        case BYTESLIST:
                            be.skipStringList();
                            break;
                        default:
                            be.skip(schema.getLength());
                    }
                }
            }
            return be.getByteArray();
        } else {
            throw new RuntimeException("Schema version Wrong!");
        }
    }

    public byte[] encodeKey(Object[] record) throws IOException {
        return internalEncodeKey(record).getByteArray();
    }

    public byte[] encodeKey(byte[] record, int[] index, Object[] columns) throws IOException {
        BinaryEncoder be = new BinaryEncoder(record, perRecordKeySize);
        be.skipByte();
        be.skipBytes();
        if (be.readShort() == this.schemaVersion) {
            List<Integer> indexList
                = Arrays.stream(index).boxed().collect(Collectors.toList());
            for (DingoSchema schema : schemas) {
                int columnIndex = indexList.indexOf(schema.getIndex());
                if (columnIndex >= 0) {
                    switch (schema.getType()) {
                        case BOOLEAN:
                            be.writeBoolean(Utils.processNullColumn(schema, columns[columnIndex]));
                            break;
                        case BOOLEANLIST:
                            be.updateBooleanList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case SHORT:
                            be.writeKeyShort(Utils.processNullColumn(schema, columns[columnIndex]));
                            break;
                        case SHORTLIST:
                            be.updateShortList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case INTEGER:
                            be.writeKeyInt(Utils.processNullColumn(schema, columns[columnIndex]));
                            break;
                        case INTEGERLIST:
                            be.updateIntegerList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case FLOAT:
                            be.writeKeyFloat(Utils.processNullColumn(schema, columns[columnIndex]));
                            break;
                        case FLOATLIST:
                            be.updateFloatList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case LONG:
                            be.writeKeyLong(Utils.processNullColumn(schema, columns[columnIndex]));
                            break;
                        case LONGLIST:
                            be.updateLongList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case DOUBLE:
                            be.writeKeyDouble(Utils.processNullColumn(schema, columns[columnIndex]));
                            break;
                        case DOUBLELIST:
                            be.updateDoubleList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case BYTES:
                            be.updateKeyBytes(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case BYTESLIST:
                            be.updateBytesList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case STRING:
                            be.updateKeyString(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        case STRINGLIST:
                            be.updateStringList(Utils
                                .processNullColumn(schema, columns[columnIndex]));
                            break;
                        default:
                    }
                } else {
                    switch (schema.getType()) {
                        case BOOLEANLIST:
                            be.skipBooleanList();
                            break;
                        case SHORTLIST:
                            be.skipShortList();
                            break;
                        case INTEGERLIST:
                            be.skipIntegerList();
                            break;
                        case FLOATLIST:
                            be.skipFloatList();
                            break;
                        case LONGLIST:
                            be.skipLongList();
                            break;
                        case DOUBLELIST:
                            be.skipDoubleList();
                            break;
                        case BYTES:
                            be.skipKeyBytes();
                            break;
                        case STRING:
                            be.skipKeyString();
                            break;
                        case BYTESLIST:
                            be.skipBytesList();
                            break;
                        case STRINGLIST:
                            be.skipStringList();
                            break;
                        default:
                            be.skip(schema.getLength());
                    }
                }
            }
            return be.getByteArray();
        } else {
            throw new RuntimeException("Schema version Wrong!");
        }
    }

    public byte[] encodeKeyWithoutLength(Object[] record) throws IOException {
        return internalEncodeKey(record).getByteArrayWithoutLength();
    }

    private BinaryEncoder internalEncodeKey(Object[] record) throws IOException {
        BinaryEncoder be = new BinaryEncoder(new byte[approPerRecordSize], new byte[perRecordKeySize]);
        be.write(finishedFlag);
        be.writeBytes(transactionId);
        be.writeShort(schemaVersion);
        for (DingoSchema schema : schemas) {
            switch (schema.getType()) {
                case BOOLEAN:
                    be.writeBoolean(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case BOOLEANLIST:
                    be.writeBooleanList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case SHORT:
                    be.writeKeyShort(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case SHORTLIST:
                    be.writeShortList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case INTEGER:
                    be.writeKeyInt(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case INTEGERLIST:
                    be.writeIntegerList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case FLOAT:
                    be.writeKeyFloat(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case FLOATLIST:
                    be.writeFloatList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case LONG:
                    be.writeKeyLong(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case LONGLIST:
                    be.writeLongList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case DOUBLE:
                    be.writeKeyDouble(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case DOUBLELIST:
                    be.writeDoubleList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case BYTES:
                    be.writeKeyBytes(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case BYTESLIST:
                    be.writeBytesList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case STRING:
                    be.writeKeyString(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                case STRINGLIST:
                    be.writeStringList(Utils.processNullColumn(schema, record[schema.getIndex()]));
                    break;
                default:
            }
        }
        return be;
    }
}
