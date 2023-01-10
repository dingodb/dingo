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

import java.util.List;
import java.util.function.Consumer;

public interface CoreListener {

    String MPU_PRIMARY = "MPU-PRIMARY";

    enum Event {
        PRIMARY,
        BACK,
        MIRROR,
        LOSE_PRIMARY,
        MIRROR_CONNECT,
        REGISTER,
        UNREGISTER
    }

    class Notifier {
        private final LinkedRunner runner;
        private final String label;

        public Notifier(String label) {
            this.label = label;
            this.runner = new LinkedRunner(label + "-notify");
        }

        public void notify(List<CoreListener> listeners, Event event, Consumer<CoreListener> notify) {
            runner.forceFollow(() -> listeners.forEach(__ -> notify(event, () -> notify.accept(__), true)));
        }

        public void notify(Event event, Runnable notify, boolean async) {
            runner.forceFollow(async ? () -> Executors.execute(label + "-" + event, notify) : notify);
        }

    }

    default void primary(long clock) {
    }

    static CoreListener primary(Consumer<Long> primary) {
        return new CoreListener() {
            @Override
            public void primary(long clock) {
                primary.accept(clock);
            }
        };
    }

    default void back(long clock) {
    }

    static CoreListener back(Consumer<Long> back) {
        return new CoreListener() {
            @Override
            public void back(long clock) {
                back.accept(clock);
            }
        };
    }

    default void mirror(long clock) {
    }

    static CoreListener mirror(Consumer<Long> mirror) {
        return new CoreListener() {
            @Override
            public void mirror(long clock) {
                mirror.accept(clock);
            }
        };
    }

    default void mirrorConnect(long clock) {
    }

    static CoreListener mirrorConnect(Consumer<Long> mirrorConnect) {
        return new CoreListener() {
            @Override
            public void mirrorConnect(long clock) {
                mirrorConnect.accept(clock);
            }
        };
    }

    default void losePrimary(long clock) {
    }

    static CoreListener losePrimary(Consumer<Long> losePrimary) {
        return new CoreListener() {
            @Override
            public void losePrimary(long clock) {
                losePrimary.accept(clock);
            }
        };
    }

}
