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

package io.dingodb.mpu.core;

import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Unsafe;
import io.dingodb.mpu.instruction.Instruction;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import lombok.experimental.FieldNameConstants;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Accessors(fluent = true, chain = true)
@FieldNameConstants
class InstructionChain implements Unsafe {

    private static final String NAME = "instruction-chain";
    private static final UnsafeAccessor UNSAFE = Unsafe.getAccessor();
    private static final Runnable EMPTY = () -> { };

    private static final long NEXT_OFFSET;

    static {
        try {
            NEXT_OFFSET = UNSAFE.objectFieldOffset(InstructionNode.class.getDeclaredField("next"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected class InstructionNode implements Runnable {

        public final Instruction instruction;
        public final InstructionChain chain = InstructionChain.this;

        @Delegate
        private final Runnable task;
        private final Runnable onClose;

        private InstructionNode next = null;

        public InstructionNode(Instruction instruction, Runnable task, Runnable onClose) {
            this.instruction = instruction;
            this.task = task;
            this.onClose = onClose;
        }

        protected boolean follow(InstructionNode next) {
            if (UNSAFE.compareAndSwapObject(this, NEXT_OFFSET, null, next)) {
                this.chain.last = next;
                return true;
            }
            return false;
        }
    }

    protected final String name;
    private long startClock;
    private InstructionNode current;
    private InstructionNode last;


    protected InstructionChain(long clock, String name) {
        this.name = name;
        this.startClock = clock;
        this.current = this.last = new InstructionNode(null, EMPTY, EMPTY);
    }

    protected boolean follow(Instruction instruction, Runnable task) {
        return last.follow(new InstructionNode(instruction, task, EMPTY));
    }

    protected boolean follow(Instruction instruction, Runnable task, Runnable onClose) {
        return last.follow(new InstructionNode(instruction, task, onClose));
    }

    protected void reset(long clock) {
        Optional.ifPresent(current.instruction, current = last = new InstructionNode(null, EMPTY, EMPTY));
        this.startClock = clock;
    }

    protected void close() {
        log.info("Instruction chain {} close.", name);
        while (current != null) {
            if (current.instruction == null) {
                return;
            }
            current.onClose.run();
            current = current.next;
        }
        last = null;
    }

    protected Runnable tick() {
        return current = current.next;
    }

    protected Runnable get() {
        return current;
    }

}
