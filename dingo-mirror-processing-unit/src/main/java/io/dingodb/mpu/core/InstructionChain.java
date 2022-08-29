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

import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.util.Unsafe;
import io.dingodb.mpu.instruction.Instruction;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import lombok.experimental.FieldNameConstants;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@Slf4j
@Getter
@Accessors(fluent = true, chain = true)
@FieldNameConstants
public class InstructionChain implements Unsafe {

    private static final String NAME = "instruction-chain";
    private static final UnsafeAccessor UNSAFE = Unsafe.getAccessor();
    private static final Runnable EMPTY = () -> { };

    private static final long NEXT_OFFSET;
    static {
        try {
            NEXT_OFFSET = UNSAFE.objectFieldOffset(InstructionNode.class.getDeclaredField(InstructionNode.Fields.next));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @FieldNameConstants
    public static class InstructionNode implements Runnable {

        public final Instruction instruction;
        public final InstructionChain runner;

        @Delegate
        private final Runnable task;

        private InstructionNode next = null;

        public InstructionNode(Instruction instruction, Runnable task, InstructionChain runner) {
            this.instruction = instruction;
            this.task = task;
            this.runner = runner;
        }

        protected boolean follow(InstructionNode next) {
            long expectClock = (instruction == null ? runner.startClock : instruction.clock) + 1;
            if (expectClock == next.instruction.clock) {
                if (UNSAFE.compareAndSwapObject(this, NEXT_OFFSET, null, next)) {
                    this.runner.last = next;
                    return true;
                }
            }
            return false;
        }

    }

    public final String name;
    private final long startClock;
    private InstructionNode current;
    private InstructionNode last;

    private final LinkedRunner runner;

    public InstructionChain(long clock, String name) {
        this.name = name;
        this.startClock = clock;
        this.current = this.last = new InstructionNode(null, EMPTY, this);
        this.runner = new LinkedRunner(name);
    }

    public void forceFollow(Instruction instruction, Runnable next) {
        forceFollow(new InstructionNode(instruction, next, this));
    }

    public void forceFollow(InstructionNode next) {
        while (!last.follow(next)) {
            if (last.instruction != null && next.instruction.clock < last.instruction.clock) {
                throw new RuntimeException("Next clock less than last instruction clock.");
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    public void reset() {
        while (current != null) {
            if (current.instruction == null) {
                return;
            }
            current.instruction.future.completeExceptionally(new RuntimeException("Available mirror less than 1."));
            current = current.next;
        }
        current = last = new InstructionNode(null, EMPTY, this);
    }

    public void tick() {
        runner.forceFollow(current = current.next);
    }
}
