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

import io.dingodb.common.CommonId;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.mpu.instruction.Instructions;
import io.dingodb.mpu.storage.Reader;
import io.dingodb.mpu.storage.Storage;
import io.dingodb.mpu.storage.Writer;
import io.dingodb.net.Message;
import io.dingodb.net.service.ListenService;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class Sidebar implements CoreListener {

    private final Consumer<Message> primaryListenerProxy;
    private final CompletableFuture<Void> started = new CompletableFuture<>();

    private final Core core;

    protected Sidebar(CoreMeta meta, Storage storage) {
        this(meta, null, storage, Collections.emptySet());
    }

    protected Sidebar(CoreMeta meta, List<CoreMeta> mirrors, Storage storage) {
        this(meta, mirrors, storage, Collections.emptySet());
    }

    protected Sidebar(CoreMeta meta, List<CoreMeta> mirrors, Storage storage, Set<CommonId> unionCoreIds) {
        this.core = new Core(meta, mirrors, storage, this);
        this.primaryListenerProxy = ListenService.getDefault().register(
            Stream.concat(unionCoreIds.stream(), Stream.of(meta.coreId)).collect(Collectors.toList()),
            MPU_PRIMARY,
            () -> isPrimary() ? Message.EMPTY : null
        );
        registerListener(this);
    }

    public CommonId id() {
        return core.meta.id;
    }

    public CommonId coreId() {
        return core.meta.coreId;
    }

    public CoreMeta meta() {
        return core.meta;
    }

    public boolean start() {
        start(true);
        return isPrimary();
    }

    public void start(boolean sync) {
        core.vCore.start();
        if (sync) {
            started.join();
        }
    }

    public void close() {
        core.vCore.close();
    }

    public void destroy() {
        ListenService.getDefault().unregister(core.vCore.meta.coreId, MPU_PRIMARY);
        core.destroy();
    }

    public void registerListener(CoreListener listener) {
        core.vCore.registerListener(listener);
    }

    public void unregisterListener(CoreListener listener) {
        core.vCore.unregisterListener(listener);
    }

    public PhaseAck exec(int instructions, int opcode, Object... operand) {
        return core.vCore.exec(instructions, opcode, operand);
    }

    public <V> V view(int instructions, int opcode, Object... operand) {
        return core.vCore.view(instructions, opcode, operand);
    }

    public boolean isPrimary() {
        return core.vCore.isPrimary();
    }

    public boolean isMirror() {
        return core.vCore.isMirror();
    }

    public CoreMeta getPrimary() {
        return core.vCore.getPrimary();
    }

    @Override
    public void primary(long clock) {
        started.complete(null);
        Executors.execute("primary-notify", () -> primaryListenerProxy.accept(Message.EMPTY));
    }

    @Override
    public void back(long clock) {
        ListenService.getDefault().clear(id(), MPU_PRIMARY);
    }

    @Override
    public void mirror(long clock) {
        started.complete(null);
    }

    protected void addVCore(Core core) {
        this.core.vCores.put(core.meta.coreId, core);
    }

    protected void addVSidebar(Sidebar sidebar) {
        addVCore(sidebar.core);
    }

    protected Core getVCore(CommonId id) {
        return core.vCores.get(id);
    }

    protected Sidebar getVSidebar(CommonId id) {
        return getVCore(id).sidebar;
    }

    protected Core delVCore(CommonId id) {
        return core.vCores.remove(id);
    }

    protected Sidebar delVSidebar(CommonId id) {
        return core.vCores.remove(id).sidebar;
    }

    protected void updateMeta(byte[] key, byte[] value) {
        exec(MetaInstructions.id, 1, key, value);
    }

    protected void getMeta(byte[] key) {
        exec(MetaInstructions.id, 0, key);
    }

    private static class MetaInstructions<C extends Context> implements Instructions {

        private static final int id = 11;

        @Override
        public Processor<?, C> processor(int opcode) {
            switch (opcode) {
                case 0: {
                    return __ -> {
                        try (Reader reader = __.core.vCore.storage.metaReader()) {
                            return reader.get((byte[]) __.operand(0));
                        }
                    };
                }
                case 1: {
                    return (VoidProcessor<?, C>) __ -> {
                        try (Writer writer = __.core.vCore.storage.metaWriter(__.instruction)) {
                            writer.set(__.operand(0), __.operand(1));
                        }
                    };
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + opcode);
            }
        }

    }

}
