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
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.mpu.Constant;
import io.dingodb.mpu.api.InternalApi;
import io.dingodb.mpu.instruction.Instruction;
import io.dingodb.mpu.protocol.SelectReturn;
import io.dingodb.mpu.protocol.SyncChannel;
import io.dingodb.mpu.protocol.TagClock;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

import static io.dingodb.mpu.Constant.NET;
import static io.dingodb.mpu.api.InternalApi.connectMirror;
import static io.dingodb.mpu.protocol.SelectReturn.ERROR;
import static io.dingodb.mpu.protocol.SelectReturn.NO;
import static io.dingodb.mpu.protocol.SelectReturn.OK;

@Slf4j
class InstructionSyncChannel implements Channel, MessageListener {

    @Delegate
    private Channel channel;
    private VCore core;
    private ControlUnit controlUnit;
    private CoreMeta mirror;
    private LinkedRunner sendRunner;
    private InstructionChain executeChain;
    private LinkedRunner chainRunner;
    private long clock;
    private long syncClock;

    public InstructionSyncChannel(VCore core, CoreMeta mirror, long clock) {
        this.core = core;
        this.mirror = mirror;
        this.sendRunner = new LinkedRunner(mirror.label + "-send-runner");
        this.executeChain = new InstructionChain(clock, mirror.label + "-instruction-chain");
        this.chainRunner = new LinkedRunner(mirror.label + "-synced-runner");
        this.clock = clock;
        this.syncClock = clock;
    }

    public SelectReturn connect() {
        try {
            log.info("Sync channel {} -> {} connect.", core.meta.label, mirror.label);
            CompletableFuture<Void> future = new CompletableFuture<>();
            this.channel = NET.newChannel(mirror.location);
            channel.setCloseListener(ch -> future.completeExceptionally(new RuntimeException("Channel closed.")));
            SelectReturn ret = connectMirror(mirror.location, new SyncChannel(channel.channelId(), core.meta, clock));
            if (ret != OK) {
                this.channel.close();
                log.info("Sync channel {} -> {} connect failed, return {}.", core.meta.label, mirror.label, ret);
                return NO;
            }
            channel.setMessageListener((_1, _2) -> future.complete(null));
            channel.send(new Message(mirror.label, ByteArrayUtils.EMPTY_BYTES));
            future.join();
            channel.setCloseListener(this::onClose);
            channel.setMessageListener(this);
            syncClock = InternalApi.askClock(mirror.location, mirror.coreId);
            log.info("Connected mirror {}, mirror sync clock [{}]", mirror.label, syncClock);
            return OK;
        } catch (Exception e) {
            if (channel != null && !channel.isClosed()) {
                channel.close();
            }
            log.error("Sync channel {} -> {} connect failed.", core.meta.label, mirror.label, e);
            return ERROR;
        }
    }

    public synchronized void assignControlUnit(ControlUnit controlUnit) {
        if (channel == null || channel.isClosed()) {
            log.info("Assign control unit failed, channel is null or channel closed.");
            return;
        }
        if (this.controlUnit == null) {
            this.controlUnit = controlUnit;
            controlUnit.onMirrorConnect(mirror, this);
        }
    }

    private void onClose(Channel channel) {
        chainRunner.forceFollow(() -> executeChain.clear(false));
        Optional.ifPresent(controlUnit, __ -> __.onMirrorClose(mirror));
    }

    @Override
    public void onMessage(Message message, Channel channel) {
        byte[] content = message.content();
        TagClock tagClock = TagClock.decode(content);
        switch (tagClock.tag) {
            case Constant.T_SYNC: {
                executeChain.tick();
                return;
            }
            default: {
                close();
                throw new IllegalStateException("Unexpected value: " + tagClock.tag);
            }
        }
    }

    public void sync(Instruction instruction) {
        if (isClosed()) {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("Sync instruction to {}, clock: {}", channel.remoteLocation(), instruction.clock);
        }
        sendRunner.forceFollow(() -> {
            if (!isClosed()) {
                try {
                    while (syncClock < instruction.clock - 1) {
                        syncClock++;
                        byte[] reappearInstruction = core.storage.reappearInstruction(syncClock);
                        if (reappearInstruction == null) {
                            core.storage.transferTo(mirror).join();
                            syncClock = InternalApi.askClock(mirror.location, mirror.coreId);
                        } else {
                            reappearInstruction[0] = Constant.T_EXECUTE_INSTRUCTION;
                            channel.send(new Message(Message.EMPTY_TAG, reappearInstruction));
                        }
                        chainRunner.forceFollow(() -> executeChain.reset(syncClock, false));
                    }
                    executeChain.forceFollow(instruction, () -> controlUnit.onSynced(mirror, instruction));
                    channel.send(new Message(Message.EMPTY_TAG, instruction.encode()), true);
                    syncClock = instruction.clock;
                } catch (Exception e) {
                    log.error("Sync to {} error.", mirror.label, e);
                    close();
                }
            }
        });
    }

    public void executed(long clock) {
        if (isClosed()) {
            return;
        }
        channel.send(new Message(Message.EMPTY_TAG, new TagClock(Constant.T_EXECUTE_CLOCK, clock).encode()));
    }

}
