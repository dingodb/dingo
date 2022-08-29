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

import io.dingodb.mpu.Constant;
import io.dingodb.mpu.instruction.Instruction;
import io.dingodb.mpu.protocol.TagClock;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(chain = true, fluent = true)
public class Mirror implements MessageListener {

    @Getter
    private final CoreMeta primary;
    private final Core core;
    @Delegate
    private final Channel channel;

    public Mirror(CoreMeta primary, Core core, long clock, Channel channel) {
        this.primary = primary;
        this.core = core;
        this.channel = channel;
        log.info("New mirror channel from {} for {}.", primary.location, core.meta.label);
    }

    @Override
    public void onMessage(Message message, Channel channel) {
        byte type = message.content()[0];
        switch (type) {
            case Constant.T_INSTRUCTION: {
                try {
                    Instruction instruction = Instruction.decode(message.content());
                    log.info("Sync {}", instruction.clock);
                    core.storage.saveInstruction(instruction.clock, message.content());
                    channel.send(new Message(null, new TagClock(Constant.T_SYNC, instruction.clock).encode()));
                    core.executionUnit.execute(instruction);
                } catch (Exception e) {
                    log.error("Sync instruction from {} error.", channel.remoteLocation(), e);
                    channel.close();
                }
                return;
            }
            case Constant.T_EXECUTE_INSTRUCTION: {
                try {
                    Instruction instruction = Instruction.decode(message.content());
                    log.info("Exec {}", instruction.clock);
                    core.storage.saveInstruction(instruction.clock, message.content());
                    core.executionUnit.execute(instruction);
                    return;
                } catch (Exception e) {
                    log.error("Sync and execute instruction from {} error.", channel.remoteLocation(), e);
                    channel.close();
                }
            }
            default: {
                close();
                throw new IllegalStateException("Unexpected value: " + type);
            }
        }
    }

}
