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

import java.util.function.Function;

public class TableInstructions implements io.dingodb.mpu.instruction.Instructions {

    public static final int id = 5;
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

    private final Function<byte[], Object[]>[] decoders = new Function[SIZE];

    static {
        //InstructionSetRegistry.register(TableInstructions.id, TableInstructions.INSTANCE);
        INSTANCE.processors[START_PART] = new Processor() {
            @Override
            public <V> V process(Reader reader, Writer writer, Object... operand) {
                TablePart part = ProtostuffCodec.read((byte[]) operand[0]);
                TableApi.INSTANCE.get(part.getTable()).startPartition(part);
                return null;
            }
        };
        INSTANCE.processors[START_INDEX] = new Processor() {
            @Override
            public <V> V process(Reader reader, Writer writer, Object... operand) {
                Index index = ProtostuffCodec.read((byte[]) operand[0]);
                TableApi.INSTANCE.get(index.getTable()).startIndex(index);
                return null;
            }
        };
        INSTANCE.processors[DROP_INDEX] = new Processor() {
            @Override
            public <V> V process(Reader reader, Writer writer, Object... operand) {
                Index index = (Index) operand[0];
                TableSidebar tableSidebar = TableApi.INSTANCE.get(index.getId());
                tableSidebar.dropIndex(index, false);
                writer.erase(index.getId().encode());
                TableDefinition definition = tableSidebar.getDefinition();
                definition.removeIndex(index.getName());
                writer.set(index.getTable().encode(), ProtostuffCodec.write(definition));
                return null;
            }
        };
        INSTANCE.processors[UPDATE_DEFINITION] = new Processor() {
            @Override
            public <V> V process(Reader reader, Writer writer, Object... operand) {
                CommonId tableId = (CommonId) operand[0];
                writer.set(tableId.encode(), ProtostuffCodec.write(operand[1]));
                TableApi.INSTANCE.get(tableId).updateDefinition((TableDefinition) operand[1], false);
                return null;
            }
        };
    }

    @Override
    public void processor(int opcode, Processor processor) {
        processors[opcode] = processor;
    }

    @Override
    public Processor processor(int opcode) {
        return processors[opcode];
    }

    @Override
    public void decoder(int opcode, Function<byte[], Object[]> decoder) {

    }

    @Override
    public Function<byte[], Object[]> decoder(int opcode) {
        return null;
    }
}
