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
import io.dingodb.common.util.Optional;
import io.dingodb.mpu.instruction.EmptyInstructions;
import io.dingodb.mpu.instruction.Instruction;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Accessors(chain = true, fluent = true)
class ControlUnit {

    public final VCore core;
    public final boolean local;
    public final long startClock;

    private CoreMeta first;
    private CoreMeta second;

    private InstructionSyncChannel firstChannel;
    private InstructionSyncChannel secondChannel;

    private LinkedRunner prepareRunner;
    private LinkedRunner syncRunner;
    private LinkedRunner executeRunner;
    private InstructionChain chain;

    private boolean closed = false;
    private final boolean extClock = false;

    private AtomicLong syncedClock;

    protected long clock;

    ControlUnit(VCore core, long clock, CoreMeta first, CoreMeta second) {
        this.core = core;
        this.first = first;
        this.second = second;
        this.startClock = clock;
        this.clock = clock;
        this.syncedClock = new AtomicLong(clock);
        this.prepareRunner = new LinkedRunner(core.meta.label + "-prepare");
        this.syncRunner = new LinkedRunner(core.meta.label + "-sync");
        this.executeRunner = new LinkedRunner(core.meta.label + "-execute");
        this.chain = new InstructionChain(clock, core.meta.label + "-instruction-chain");
        this.local = first == null;
    }

    protected void close() {
        closed = true;
        log.info("{} control unit close on {}.", core.meta.label, clock);
        core.onControlUnitClose();
        if (firstChannel != null) {
            firstChannel.close();
        }
        if (secondChannel != null) {
            secondChannel.close();
        }
        chain.close();
    }

    protected boolean isClosed() {
        return closed;
    }

    protected synchronized void onMirrorConnect(CoreMeta mirror, InstructionSyncChannel channel) {
        if (closed) {
            throw new RuntimeException("Control unit closed.");
        }
        if (mirror.label.equals(first.label)) {
            firstChannel = channel;
        } else {
            secondChannel = channel;
        }
        log.info("{} control unit connect {} on {}.", core.meta.label, mirror.label, clock);
        PhaseAck ack = process(EmptyInstructions.id, EmptyInstructions.EMPTY);
        ack.join();
        log.info("{} control unit connected {} on {}.", core.meta.label, mirror.label, ack.clock);
    }

    protected synchronized void onMirrorClose(CoreMeta mirror) {
        if (closed) {
            return;
        }
        log.info("{} control unit close connection {} on {}.", core.meta.label, mirror.label, clock);
        if (mirror.label.equals(first.label)) {
            firstChannel = null;
        } else {
            secondChannel = null;
        }
        if (firstChannel == null && secondChannel == null) {
            close();
        }
    }

    protected PhaseAck process(byte instructions, short opcode, Object... operand) {
        if (extClock) {
            throw new UnsupportedOperationException("Need clock.");
        }
        PhaseAck ack = new PhaseAck();
        prepareRunner.forceFollow(() -> this.prepare(ack, instructions, opcode, operand));
        return ack;
    }

    private PhaseAck prepare(PhaseAck ack, byte instructions, short opcode, Object... operand) {
        Instruction instruction = new Instruction(++clock, instructions, opcode, operand);
        ack.completeClock(clock);
        syncRunner.forceFollow(() -> sync(ack, instruction));
        return ack;
    }

    protected PhaseAck process(PhaseAck ack, long clock, byte instructions, short opcode, Object... operand) {
        if (!extClock) {
            throw new UnsupportedOperationException("Not ext clock mode.");
        }
        Instruction instruction = new Instruction(clock, instructions, opcode, operand);
        ack.completeClock(clock);
        syncRunner.forceFollow(() -> sync(ack, instruction));
        sync(ack, instruction);
        return ack;
    }

    private void sync(PhaseAck ack, Instruction instruction) {
        if (local) {
            core.executionUnit.execute(instruction, ack);
            return;
        }
        if (isClosed()) {
            ack.completeExceptionally(new RuntimeException("Control unit closed."));
            return;
        }
        core.storage.saveInstruction(instruction.clock, instruction.encode());
        if (
            chain.follow(
                instruction,
                () -> core.executionUnit.execute(instruction, ack),
                () -> ack.completeExceptionally(new RuntimeException("Unavailable, control unit closed."))
            )
        ) {
            Optional.ifPresent(firstChannel, __ -> __.sync(instruction));
            Optional.ifPresent(secondChannel, __ -> __.sync(instruction));
        } else {
            log.warn("Chain follow failed, clock: {}, reprocess instruction.", instruction.clock);
            ack.completeExceptionally(new RuntimeException(
                "Execute error, clock follow failed, clock: " + instruction.clock
            ));
        }
    }

    protected void onSynced(CoreMeta mirror, Instruction instruction) {
        if (syncedClock.compareAndSet(instruction.clock - 1, instruction.clock)) {
            executeRunner.forceFollow(chain::tick);
        } else {
            core.storage.clearClock(instruction.clock);
            firstChannel.executed(instruction.clock);
            secondChannel.executed(instruction.clock);
        }
    }

}
