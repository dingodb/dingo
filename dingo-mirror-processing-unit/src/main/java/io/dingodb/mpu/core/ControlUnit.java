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
import io.dingodb.mpu.Constant;
import io.dingodb.mpu.api.InternalApi;
import io.dingodb.mpu.instruction.Instruction;
import io.dingodb.mpu.instruction.InternalInstructions;
import io.dingodb.net.Message;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
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

    private AtomicLong syncedClock;

    private long clock;

    ControlUnit(Core core, long clock, CoreMeta first, CoreMeta second) {
        this.core = core;
        this.first = first;
        this.second = second;
        this.startClock = clock;
        this.clock = clock;
        this.syncedClock = new AtomicLong(clock);
        this.executeRunner = new LinkedRunner(core.meta.label + "-execute");
        this.chain = new InstructionChain(clock, core.meta.label + "-instruction-chain");
        this.firstRunner = new LinkedRunner(first.label + "-sync-runner");
        this.secondRunner = new LinkedRunner(second.label + "-sync-runner");
        this.local = first == null;
    }

    public void close() {
        closed = true;
        log.info("{} control unit close on {}.", core.meta.label, clock);
        core.onControlUnitClose();
    }

    public boolean isClosed() {
        return closed;
    }

    public synchronized void onMirrorConnect(CoreMeta mirror, InstructionSyncChannel channel) {
        if (closed) {
            throw new RuntimeException("Control unit closed.");
        }
        long mirrorClock = InternalApi.askClock(mirror.location, mirror.mpuId, mirror.coreId);
        while (mirrorClock < clock - 1) {
            mirrorClock++;
            if (clock - mirrorClock >= 100_0000) {
                core.storage.transferTo(mirror).join();
                continue;
            }
            byte[] reappearInstruction = core.storage.reappearInstruction(mirrorClock);
            reappearInstruction[0] = Constant.T_EXECUTE_INSTRUCTION;
            channel.send(new Message(null, reappearInstruction));
        }
        if (mirror == first) {
            firstChannel = channel;
        } else {
            secondChannel = channel;
        }
        PhaseAck<Object> ack = new PhaseAck<>();
        log.info("{} control unit connect {} on {}.", core.meta.label, mirror.label, clock);
        core.runner.forceFollow(() -> process(ack, InternalInstructions.id, InternalInstructions.EMPTY));
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

    protected <V> void process(PhaseAck<V> ack, byte instructions, short opcode, Object... operand) {
        Instruction instruction = new Instruction(++clock, instructions, opcode, operand);
        ack.clock.complete(instruction.clock);
        ack.result = (CompletableFuture<V>) instruction.future;
        if (local) {
            executeRunner.forceFollow(() -> core.executionUnit.execute(instruction, ack.result));
            return;
        }
        if (isClosed()) {
            ack.result.completeExceptionally(new RuntimeException("Control unit closed."));
            return;
        }
        core.storage.saveInstruction(instruction.clock, instruction.encode());
        executeRunner.forceFollow(() -> chain.forceFollow(instruction, () -> core.executionUnit.execute(instruction, ack.result)));
        Optional.ifPresent(firstChannel, () -> firstRunner.forceFollow(() -> firstChannel.sync(instruction)));
        Optional.ifPresent(secondChannel, () -> secondRunner.forceFollow(() -> secondChannel.sync(instruction)));
    }

    protected void onSynced(CoreMeta mirror, Instruction instruction) {
        if (syncedClock.compareAndSet(instruction.clock - 1, instruction.clock)) {
            executeRunner.forceFollow(chain::tick);
        }
    }

}
