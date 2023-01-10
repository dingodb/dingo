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
import io.dingodb.mpu.MPURegister;
import io.dingodb.mpu.api.InternalApi;
import io.dingodb.mpu.instruction.InstructionSetRegistry;
import io.dingodb.mpu.instruction.InternalInstructions;
import io.dingodb.mpu.protocol.SelectReturn;
import io.dingodb.mpu.protocol.SyncChannel;
import io.dingodb.mpu.storage.Reader;
import io.dingodb.mpu.storage.Storage;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.dingodb.common.concurrent.Executors.execute;
import static io.dingodb.mpu.Constant.NET;
import static io.dingodb.mpu.protocol.SelectReturn.NO;
import static io.dingodb.mpu.protocol.SelectReturn.OK;

@Slf4j
public class VCore {

    public final Core core;
    public final CoreMeta meta;
    public final Storage storage;
    public final CoreMeta firstMirror;
    public final CoreMeta secondMirror;

    final ExecutionUnit executionUnit;
    final CoreListener.Notifier notifier;

    @Getter
    private CoreMeta primary;
    private ControlUnit controlUnit;
    private MirrorChannel mirrorChannel;

    private boolean close;

    protected List<CoreListener> listeners = new ArrayList<>();

    protected VCore(Core core, CoreMeta meta, Storage storage) {
        this(core, meta, null, null, storage);
    }

    protected VCore(Core core, CoreMeta meta, CoreMeta firstMirror, CoreMeta secondMirror, Storage storage) {
        if (firstMirror != secondMirror && (firstMirror == null || secondMirror == null)) {
            throw new IllegalArgumentException("Mirror1 and mirror2 can't have just one that's not null.");
        }
        this.core = core;
        this.notifier = new CoreListener.Notifier(meta.label);
        this.meta = meta;
        this.firstMirror = firstMirror;
        this.secondMirror = secondMirror;
        this.storage = storage;
        this.executionUnit = new ExecutionUnit(this);
    }

    public void start() {
        log.info("Start core {} on {}.", meta.label, clock());
        if (firstMirror == null && secondMirror == null) {
            log.info("Core {} start without mirror.", meta.label);
            controlUnit = new ControlUnit(this, storage.clocked(), null, null);
            primary = meta;
            notifier.notify(listeners, CoreListener.Event.PRIMARY, __ -> __.primary(clock()));
        }
        Executors.scheduleAsync(meta.label + "-select-primary", this::selectPrimary, 1, TimeUnit.SECONDS);
    }

    protected void destroy() {
        // todo exec(InternalInstructions.id, InternalInstructions.DESTROY_OC);
        InternalInstructions.process(this, InternalInstructions.DESTROY_OC);
        MPURegister.unregister(meta.coreId);
    }

    public void close() {
        try {
            close = true;
            if (controlUnit != null) {
                controlUnit.close();
            }
            if (mirrorChannel != null) {
                mirrorChannel.close();
            }
        } catch (Exception e) {
            log.error("VCore close error. ", e);
        }
    }

    public List<CoreMeta> mirrors() {
        if (firstMirror == null) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(firstMirror, secondMirror);
        }
    }

    public void registerListener(CoreListener listener) {
        notifier.notify(CoreListener.Event.REGISTER, () -> {
            listeners.add(listener);
            if (isPrimary()) {
                notifier.notify(CoreListener.Event.PRIMARY, () -> listener.primary(clock()), true);
            }
        }, false);
    }

    public void unregisterListener(CoreListener listener) {
        notifier.notify(CoreListener.Event.UNREGISTER, () -> listeners.remove(listener), false);
    }

    public void onControlUnitClose() {
        if (close) {
            notifier.notify(listeners, CoreListener.Event.BACK, __ -> __.back(controlUnit.clock));
            return;
        }
        this.controlUnit = null;
        long clock = clock();
        notifier.notify(listeners, CoreListener.Event.BACK, __ -> __.back(controlUnit.clock));
        selectPrimary();
    }

    private boolean connectPrimary(CoreMeta primary) {
        if (close) {
            return false;
        }
        try {
            if (InternalApi.isPrimary(primary.location, meta.coreId)) {
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
            return SelectReturn.PRIMARY;
        }
        if (this.mirrorChannel != null) {
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
        log.info(
            "Ask primary {} return {}, {} return {}.",
            firstMirror.label, firstReturn, secondMirror.label, secondReturn
        );
        if ((firstReturn != OK && secondReturn != OK) || firstReturn == NO || secondReturn == NO) {
            return;
        }
        ControlUnit controlUnit = new ControlUnit(this, clock, firstMirror, secondMirror);
        InstructionSyncChannel firstChannel = new InstructionSyncChannel(this, firstMirror, clock);
        InstructionSyncChannel secondChannel = new InstructionSyncChannel(this, secondMirror, clock);
        firstReturn = firstChannel.connect();
        secondReturn = secondChannel.connect();
        log.info(
            "Sync channel connect {} return {}, {} return {}",
            firstMirror.label, firstReturn, secondMirror.label, secondReturn
        );
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
            if (mirrorChannel == null) {
                primary = this.meta;
                firstChannel.assignControlUnit(controlUnit);
                secondChannel.assignControlUnit(controlUnit);
                this.controlUnit = controlUnit;
                notifier.notify(listeners, CoreListener.Event.PRIMARY, __ -> __.primary(clock));
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
        if (mirrorChannel != null) {
            log.info("{} is primary.", mirrorChannel.primary().label);
            return;
        }
        if (connectPrimary(firstMirror) || connectPrimary(secondMirror)) {
            Executors.scheduleAsync(meta.label + "-select-primary", this::selectPrimary, 1, TimeUnit.SECONDS);
            return;
        }
        log.info("Can not connect primary, maybe current node is primary, connect mirrors.");
        connectMirrors();
        if (controlUnit == null || mirrorChannel == null) {
            Executors.scheduleAsync(meta.label + "-select-primary", this::selectPrimary, 1, TimeUnit.SECONDS);
        }
    }

    public synchronized SelectReturn connectFromPrimary(SyncChannel syncChannel) {
        if (close) {
            return NO;
        }
        log.info(
            "Receive primary sync channel from {}, remote clock: {}, local clock:{}.",
            syncChannel.primary.label, syncChannel.clock, clock()
        );
        SelectReturn selectReturn = askPrimary(syncChannel.primary, syncChannel.clock);
        if (selectReturn == OK) {
            log.info("Ask ok for {}.", syncChannel.primary.label);
            primary = syncChannel.primary;
            NET.setMessageListenerProvider(meta.label, (message, channel) -> newPrimaryChannel(syncChannel, channel));
        }
        return selectReturn;
    }

    private synchronized MirrorChannel newPrimaryChannel(SyncChannel syncChannel, Channel channel) {
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
            this.mirrorChannel = null;
            notifier.notify(listeners, CoreListener.Event.LOSE_PRIMARY, __ -> __.losePrimary(close ? -1 : clock()));
            execute(meta.label + "-select-primary", this::selectPrimary);
        });
        MirrorChannel mirrorChannel = new MirrorChannel(syncChannel.primary, this, clock(), channel);
        execute(primary.label + "-connected-primary", () -> {
            long clock = clock();
            channel.send(Message.EMPTY);
            notifier.notify(listeners, CoreListener.Event.MIRROR, __ -> __.mirror(clock));
            log.info("Connected primary {} success on {}.", syncChannel.primary.label, clock);
            if (close) {
                mirrorChannel.close();
            }
        });
        return this.mirrorChannel = mirrorChannel;
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
        return controlUnit.process((byte) instructions, (short) opcode, operand);
    }

    public <V> V view(int instructions, int opcode, Object... operand) {
        if (!isAvailable()) {
            throw new UnsupportedOperationException("Not available.");
        }
        try (
            Reader reader = storage.reader();
            io.dingodb.mpu.instruction.Context context = new io.dingodb.mpu.instruction.Context() {
                @Override
                public Reader reader() {
                    return reader;
                }

                @Override
                public Object[] operand() {
                    return operand;
                }

                @Override
                public <O> O operand(int index) {
                    return (O) operand[index];
                }
            }
        ) {
            return InstructionSetRegistry.instructions(instructions).process(opcode, context);
        }
    }

}
