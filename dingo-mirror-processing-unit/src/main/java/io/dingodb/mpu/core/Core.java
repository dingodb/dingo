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

import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.store.Part;
import io.dingodb.mpu.api.InternalApi;
import io.dingodb.mpu.instruction.InstructionSetRegistry;
import io.dingodb.mpu.instruction.Instructions;
import io.dingodb.mpu.instruction.InternalInstructions;
import io.dingodb.mpu.protocol.SelectReturn;
import io.dingodb.mpu.protocol.SyncChannel;
import io.dingodb.mpu.storage.Reader;
import io.dingodb.mpu.storage.Storage;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static io.dingodb.common.concurrent.Executors.execute;
import static io.dingodb.mpu.Constant.NET;
import static io.dingodb.mpu.protocol.SelectReturn.NO;
import static io.dingodb.mpu.protocol.SelectReturn.OK;
import static io.dingodb.mpu.protocol.SelectReturn.PRIMARY;

@Slf4j
public class Core {

    public final CoreMeta meta;
    public final Storage storage;
    public final MirrorProcessingUnit mpu;
    public final CoreMeta firstMirror;
    public final CoreMeta secondMirror;

    final ExecutionUnit executionUnit;
    final LinkedRunner runner;

    @Getter
    private CoreMeta primary;
    private ControlUnit controlUnit;
    private Mirror mirror;

    private boolean close;

    private List<CoreListener> listeners = new CopyOnWriteArrayList<>();

    public Core(MirrorProcessingUnit mpu, CoreMeta meta, CoreMeta firstMirror, CoreMeta secondMirror, Storage storage) {
        if (firstMirror != secondMirror && (firstMirror == null || secondMirror == null)) {
            throw new IllegalArgumentException("Mirror1 and mirror2 can't have just one that's not null.");
        }
        this.runner = new LinkedRunner(meta.label);
        this.mpu = mpu;
        this.meta = meta;
        this.firstMirror = firstMirror;
        this.secondMirror = secondMirror;
        this.storage = storage;
        this.executionUnit = new ExecutionUnit(this);
    }

    public void start() {
        log.info("Start core {}", meta.label);
        if (firstMirror == null && secondMirror == null) {
            log.info("Core {} start without mirror.", meta.label);
            controlUnit = new ControlUnit(this, storage.clocked(), null, null);
            primary = meta;
            listeners.forEach(__ -> execute(meta.label + "-primary", () -> __.primary(clock())));
        }
        Executors.scheduleAsync(meta.label + "-select-primary", this::selectPrimary, 1, TimeUnit.SECONDS);
    }

    public void destroy() {
        // todo exec(InternalInstructions.id, InternalInstructions.DESTROY_OC);
        InternalInstructions.process(this, InternalInstructions.DESTROY_OC);
    }

    public void close() {
        close = true;
        if (controlUnit != null) {
            controlUnit.close();
        }
        if (mirror != null) {
            mirror.close();
        }
    }

    public void registerListener(CoreListener listener) {
        listeners.add(listener);
    }

    public void unregisterListener(CoreListener listener) {
        listeners.remove(listener);
    }

    public void onControlUnitClose() {
        if (close) {
            listeners.forEach(__ -> execute(meta.label + "-back", () -> __.back(controlUnit.clock)));
            return;
        }
        this.controlUnit = null;
        long clock = clock();
        listeners.forEach(__ -> execute(meta.label + "-back", () -> __.back(clock)));
        selectPrimary();
    }

    private boolean connectPrimary(CoreMeta primary) {
        if (close) {
            return false;
        }
        try {
            if (InternalApi.isPrimary(primary.location, meta.mpuId, meta.coreId)) {
                log.info("Ask primary {} return ok.", primary.label);
                InternalApi.requestConnect(primary.location, meta);
                return true;
            }
        } catch (Exception ignored) {
        }
        log.info("Connect primary {} failed, it is not primary or no network connection", primary.label);
        return false;
    }

    public SelectReturn askPrimary(CoreMeta mirror, long clock) {
        if (close) {
            return NO;
        }
        if (controlUnit != null) {
            return PRIMARY;
        }
        if (this.mirror != null) {
            return NO;
        }
        long localClock = clock();
        if (clock > localClock || (clock == localClock && mirror.id.compareTo(meta.id) > 0)) {
            return OK;
        }
        return NO;
    }

    public void connectMirrors() {
        if (close) {
            return;
        }
        long clock = clock();
        SelectReturn firstReturn = InternalApi.askPrimary(firstMirror.location, meta, clock);
        SelectReturn secondReturn = InternalApi.askPrimary(secondMirror.location, meta, clock);
        if ((firstReturn != OK && secondReturn != OK) || firstReturn == NO || secondReturn == NO) {
            return;
        }
        ControlUnit controlUnit = new ControlUnit(this, clock, firstMirror, secondMirror);
        InstructionSyncChannel firstChannel = new InstructionSyncChannel(this, firstMirror, clock);
        InstructionSyncChannel secondChannel = new InstructionSyncChannel(this, secondMirror, clock);
        firstReturn = firstChannel.connect();
        secondReturn = secondChannel.connect();
        if (firstReturn != OK && secondReturn != OK) {
            return;
        }
        if (firstReturn == NO) {
            secondChannel.close();
            return;
        }
        if (secondReturn == NO) {
            firstChannel.close();
            return;
        }
        synchronized (this) {
            if (mirror == null) {
                primary = this.meta;
                firstChannel.assignControlUnit(controlUnit);
                secondChannel.assignControlUnit(controlUnit);
                this.controlUnit = controlUnit;
                listeners.forEach(__ -> execute(meta.label + "-primary", () -> __.primary(clock)));
            } else {
                if (firstReturn == OK) {
                    firstChannel.close();
                }
                if (secondReturn == OK) {
                    secondChannel.close();
                }
            }
        }
    }

    public void selectPrimary() {
        if (close) {
            return;
        }
        log.info("{} select primary.", meta.label);
        if (controlUnit != null) {
            log.info("{} is primary.", meta.label);
            return;
        }
        if (mirror != null) {
            log.info("{} is primary.", mirror.primary().label);
            return;
        }
        if (connectPrimary(firstMirror) || connectPrimary(secondMirror)) {
            Executors.scheduleAsync(meta.label + "-select-primary", this::selectPrimary, 1, TimeUnit.SECONDS);
            return;
        }
        log.info("Can not connect primary, maybe current node is primary, connect mirrors.");
        connectMirrors();
        if (controlUnit == null || mirror == null) {
            Executors.scheduleAsync(meta.label + "-select-primary", this::selectPrimary, 1, TimeUnit.SECONDS);
        }
    }

    public synchronized SelectReturn connectFromPrimary(SyncChannel syncChannel) {
        if (close) {
            return NO;
        }
        log.info("Receive primary sync channel from {}.", syncChannel.primary.label);
        SelectReturn selectReturn = askPrimary(syncChannel.primary, syncChannel.clock);
        if (selectReturn == OK) {
            log.info("Ask ok for {}.", syncChannel.primary.label);
            primary = syncChannel.primary;
            NET.setMessageListenerProvider(meta.label, (message, channel) -> newPrimaryChannel(syncChannel, channel));
        }
        return selectReturn;
    }

    private synchronized Mirror newPrimaryChannel(SyncChannel syncChannel, Channel channel) {
        log.info("Receive primary message from {}.", syncChannel.primary.label);
        if (!syncChannel.primary.equals(primary)) {
            channel.close();
            log.info(
                "Receive primary message from {}, but not eq current primary {}.",
                syncChannel.primary.label, primary.label
            );
            return null;
        }
        channel.setCloseListener(ch -> {
            log.info("Mirror connection from {} closed.", syncChannel.primary.label);
            this.mirror = null;
            listeners.forEach(__ -> execute(meta.label + "-lose-primary", () -> __.losePrimary(close ? -1 : clock())));
            execute(meta.label + "-select-primary", this::selectPrimary);
        });
        Mirror mirror = new Mirror(syncChannel.primary, this, clock(), channel);
        execute(primary.label + "-connected-primary", () -> {
            long clock = clock();
            channel.send(Message.EMPTY);
            listeners.forEach(__ -> execute(meta.label + "-lose-primary", () -> __.mirror(clock)));
            log.info("Connected primary {} success on {}.", syncChannel.primary.label, clock);
            if (close) {
                mirror.close();
            }
        });
        return this.mirror = mirror;
    }


    public synchronized void requestConnect(CoreMeta mirror) {
        if (close) {
            return;
        }
        log.info("Receive connect request from {}.", mirror.label);
        execute("connect-mirror-" + mirror.label, () -> {
            if (controlUnit == null) {
                return;
            }
            log.info("Connect mirror {}.", mirror.label);
            InstructionSyncChannel channel = new InstructionSyncChannel(this, mirror, clock());
            channel.connect();
            channel.assignControlUnit(controlUnit);
        });
    }

    public boolean isPrimary() {
        return controlUnit != null;
    }

    public boolean isAvailable() {
        return !close && isPrimary() && !controlUnit.isClosed();
    }

    public long clock() {
        return storage.clocked();
    }

    public PhaseAck exec(int instructions, int opcode, Object... operand) {
        if (!isAvailable()) {
            throw new UnsupportedOperationException("Not available.");
        }
        PhaseAck ack = new PhaseAck();
        runner.forceFollow(() -> controlUnit.process(ack, (byte) instructions, (short) opcode, operand));
        return ack;
    }

    public <V> V view(int instructions, int opcode, Object... operand) {
        if (!isAvailable()) {
            throw new UnsupportedOperationException("Not available.");
        }
        try (Reader reader = storage.reader()) {
            return InstructionSetRegistry.instructions(instructions).process(reader, null, opcode, operand);
        }
    }

}
