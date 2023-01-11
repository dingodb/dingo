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

package io.dingodb.server.executor.sidebar;

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.mpu.storage.Reader;
import io.dingodb.mpu.storage.Writer;
import io.dingodb.server.executor.api.TableApi;
import io.dingodb.server.protocol.meta.Index;
import io.dingodb.server.protocol.meta.TablePart;

public class TableInstructions implements io.dingodb.mpu.instruction.Instructions {

    public static final int id = 33;
    public static final TableInstructions INSTANCE = new TableInstructions();
    private static final int SIZE = 32;

    public static final int UPDATE_DEFINITION = 1;
    public static final int START_PART = 11;
    public static final int CLOSE_PART = 12;
    public static final int DROP_PART = 13;
    public static final int START_INDEX = 21;
    public static final int CLOSE_INDEX = 22;
    public static final int DROP_INDEX = 23;


    private final io.dingodb.mpu.instruction.Instructions.Processor[] processors = new Processor[SIZE];

    static {
        INSTANCE.processors[START_PART] = (VoidProcessor) context -> {
            TablePart part = ProtostuffCodec.read((byte[]) context.operand(0));
            TableApi.INSTANCE.get(part.getTable()).startPartition(part);
        };
        INSTANCE.processors[DROP_PART] = (VoidProcessor) context -> {
            // todo
        };
        INSTANCE.processors[START_INDEX] = (VoidProcessor) context -> {
            Index index = ProtostuffCodec.read((byte[]) context.operand(0));
            TableApi.INSTANCE.get(index.getTable()).startIndex(index);
        };
        INSTANCE.processors[DROP_INDEX] = (VoidProcessor) context -> {
            Index index = context.operand(0);
            TableSidebar tableSidebar = TableApi.INSTANCE.get(index.getTable());
            tableSidebar.dropIndex(index, false);
            context.writer().erase(index.getId().encode());
            TableDefinition definition = tableSidebar.getDefinition();
            definition.removeIndex(index.getName());
            context.writer().set(index.getTable().encode(), ProtostuffCodec.write(definition));
        };
        INSTANCE.processors[UPDATE_DEFINITION] = (VoidProcessor) context -> {
            CommonId tableId = context.operand(0);
            context.writer().set(tableId.encode(), ProtostuffCodec.write(context.operand(1)));
            TableApi.INSTANCE.get(tableId).updateDefinition(context.operand(1), false);
        };

    }

    @Override
    public Processor processor(int opcode) {
        return processors[opcode];
    }

}
