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

package io.dingodb.common.concurrent;

import io.dingodb.common.util.Unsafe;
import io.dingodb.common.util.Utils;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.experimental.FieldNameConstants;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Accessors(fluent = true, chain = true)
@FieldNameConstants
public final class LinkedRunner implements Unsafe {

    private static final String NAME = "linked-runner";
    private static final Unsafe.UnsafeAccessor UNSAFE = Unsafe.getAccessor();
    private static final Runnable EMPTY = () -> { };

    private static final long COMPLETE_OFFSET;
    private static final long NEXT_OFFSET;

    static {
        try {
            COMPLETE_OFFSET = UNSAFE.objectFieldOffset(RunnerNode.class.getDeclaredField(RunnerNode.Fields.complete));
            NEXT_OFFSET = UNSAFE.objectFieldOffset(RunnerNode.class.getDeclaredField(RunnerNode.Fields.next));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @FieldNameConstants
    public static class RunnerNode implements Runnable, Unsafe {

        private final Runnable task;
        private final LinkedRunner runner;

        private int complete = 0;
        private RunnerNode next = null;

        public RunnerNode(Runnable task, LinkedRunner runner) {
            this.task = task;
            this.runner = runner;
        }

        private boolean follow(RunnerNode next) {
            if (UNSAFE.compareAndSwapObject(this, NEXT_OFFSET, null, next)) {
                runner.last = next;
                if (UNSAFE.compareAndSwapInt(this, COMPLETE_OFFSET, 1, 2)) {
                    runner.submit(next);
                }
                return true;
            }
            return false;
        }

        @Override
        public void run() {
            runner.current = this;
            try {
                task.run();
            } catch (Exception e) {
                log.error("Execute task [{}] error, the exception should be handled within the task.", runner.name, e);
            }
            UNSAFE.compareAndSwapInt(this, COMPLETE_OFFSET, 0, 1);
        }

    }

    public final String name;
    private RunnerNode current;
    private RunnerNode last;

    public LinkedRunner(String name) {
        this.name = name;
        this.last = new RunnerNode(EMPTY, this);
        last.run();
    }

    public boolean hasNext() {
        return !(current == null || current == last);
    }

    public boolean follow(Runnable task) {
        return last.follow(new RunnerNode(task, this));
    }

    public boolean follow(RunnerNode next) {
        return last.follow(next);
    }

    public void forceFollow(Runnable task) {
        forceFollow(new RunnerNode(task, this));
    }

    public void forceFollow(RunnerNode next) {
        Utils.loop(() -> !last.follow(next));
    }

    private void submit(final RunnerNode node) {
        Executors.execute(name, () -> {
            RunnerNode next = node;
            while (true) {
                next.run();
                if (
                        !UNSAFE.compareAndSwapObject(next, NEXT_OFFSET, null, null)
                        && UNSAFE.compareAndSwapInt(next, COMPLETE_OFFSET, 1, 2)
                ) {
                    next = next.next;
                } else {
                    return;
                }
            }
        });
    }

}
