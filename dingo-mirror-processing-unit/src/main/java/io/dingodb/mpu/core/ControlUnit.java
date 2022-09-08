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
import io.dingodb.mpu.api.InternalApi;
import io.dingodb.mpu.instruction.EmptyInstructions;
import io.dingodb.mpu.instruction.Instruction;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Accessors(chain = true, fluent = true)
class ControlUnit {

    public final Core core;
    public final boolean local;
    public final long startClock;

    private CoreMeta first;
    private CoreMeta second;

    private InstructionSyncChannel firstChannel;
    private InstructionSyncChannel secondChannel;

    private LinkedRunner firstRunner;
    private LinkedRunner secondRunner;
    private LinkedRunner executeRunner;
    private InstructionChain chain;

    private boolean closed = false;
    private final boolean extClock = false;

    private AtomicLong syncedClock;

    protected long clock;

    ControlUnit(Core core, long clock, CoreMeta first, CoreMeta second) {
        this.core = core;
        this.first = first;
        this.second = second;
        this.startClock = clock;
        this.clock = clock;
        this.syncedClock = new AtomicLong(clock);
        this.executeRunner = new LinkedRunner(core.meta.label + "-execute");
        this.chain = new InstructionChain(clock, core.meta.label + "-instruction-chain");
        this.local = first == null;
        if (!local) {
            this.firstRunner = new LinkedRunner(first.label + "-sync-runner");
            this.secondRunner = new LinkedRunner(second.label + "-sync-runner");
        }
    }

    public void close() {
        closed = true;
        log.info("{} control unit close on {}.", core.meta.label, clock);
        chain.clear(true);
        core.onControlUnitClose();
        if (firstChannel != null) {
            firstChannel.close();
        }
        if (secondChannel != null) {
            secondChannel.close();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public synchronized void onMirrorConnect(CoreMeta mirror, InstructionSyncChannel channel) {
        if (closed) {
            throw new RuntimeException("Control unit closed.");
        }
        long mirrorClock = InternalApi.askClock(mirror.location, mirror.mpuId, mirror.coreId);
        if (mirrorClock < clock - 1) {
            channel.sync(Instruction.decode(core.storage.reappearInstruction(clock - 1)));
        }
        if (mirror == first) {
            firstChannel = channel;
        } else {
            secondChannel = channel;
        }
        PhaseAck ack = new PhaseAck();
        log.info("{} control unit connect {} on {}.", core.meta.label, mirror.label, clock);
        core.runner.forceFollow(() -> process(ack, EmptyInstructions.id, EmptyInstructions.EMPTY));
        ack.join();
    }

    public synchronized void onMirrorClose(CoreMeta mirror) {
        if (closed) {
            return;
        }
        log.info("{} control unit close connection {} on {}.", core.meta.label, mirror.label, clock);
        if (mirror == first) {
            firstChannel = null;
        } else {
            secondChannel = null;
        }
        if (firstChannel == null && secondChannel == null) {
            close();
        }
    }

    protected void process(PhaseAck ack, byte instructions, short opcode, Object... operand) {
        Instruction instruction = new Instruction(++clock, instructions, opcode, operand);
        processInstruction(ack, instruction);
    }

    protected void process(PhaseAck ack, long clock, byte instructions, short opcode, Object... operand) {
        if (!extClock) {
            throw new UnsupportedOperationException("Not ext clock mode.");
        }
        Instruction instruction = new Instruction(clock, instructions, opcode, operand);
        processInstruction(ack, instruction);
    }

    private void processInstruction(PhaseAck ack, Instruction instruction) {
        ack.result = instruction.future;
        ack.clock.complete(instruction.clock);
        if (local) {
            executeRunner.forceFollow(() -> core.executionUnit.execute(instruction, ack.result));
            return;
        }
        if (isClosed()) {
            ack.result.completeExceptionally(new RuntimeException("Control unit closed."));
            return;
        }
        core.storage.saveInstruction(instruction.clock, instruction.encode());
        executeRunner.forceFollow(() -> chain.forceFollow(
            instruction, () -> core.executionUnit.execute(instruction, ack.result)));
        Optional.ifPresent(firstChannel, () -> firstRunner.forceFollow(() -> firstChannel.sync(instruction)));
        Optional.ifPresent(secondChannel, () -> secondRunner.forceFollow(() -> secondChannel.sync(instruction)));
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
