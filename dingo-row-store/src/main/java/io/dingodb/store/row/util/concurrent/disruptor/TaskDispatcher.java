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

package io.dingodb.store.row.util.concurrent.disruptor;

import io.dingodb.raft.util.Ints;
import io.dingodb.raft.util.Requires;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.LiteTimeoutBlockingWaitStrategy;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class TaskDispatcher implements Dispatcher<Runnable> {
    private static final EventFactory<MessageEvent<Runnable>> eventFactory = MessageEvent::new;

    private final Disruptor<MessageEvent<Runnable>> disruptor;

    public TaskDispatcher(int bufSize, int numWorkers, WaitStrategyType waitStrategyType, ThreadFactory threadFactory) {
        Requires.requireTrue(bufSize > 0, "bufSize must be larger than 0");
        if (!Ints.isPowerOfTwo(bufSize)) {
            bufSize = Ints.roundToPowerOfTwo(bufSize);
        }
        WaitStrategy waitStrategy;
        switch (waitStrategyType) {
            case BLOCKING_WAIT:
                waitStrategy = new BlockingWaitStrategy();
                break;
            case LITE_BLOCKING_WAIT:
                waitStrategy = new LiteBlockingWaitStrategy();
                break;
            case TIMEOUT_BLOCKING_WAIT:
                waitStrategy = new TimeoutBlockingWaitStrategy(1000, TimeUnit.MILLISECONDS);
                break;
            case LITE_TIMEOUT_BLOCKING_WAIT:
                waitStrategy = new LiteTimeoutBlockingWaitStrategy(1000, TimeUnit.MILLISECONDS);
                break;
            case PHASED_BACK_OFF_WAIT:
                waitStrategy = PhasedBackoffWaitStrategy.withLiteLock(1000, 1000, TimeUnit.NANOSECONDS);
                break;
            case SLEEPING_WAIT:
                waitStrategy = new SleepingWaitStrategy();
                break;
            case YIELDING_WAIT:
                waitStrategy = new YieldingWaitStrategy();
                break;
            case BUSY_SPIN_WAIT:
                waitStrategy = new BusySpinWaitStrategy();
                break;
            default:
                throw new UnsupportedOperationException(waitStrategyType.toString());
        }
        this.disruptor = new Disruptor<>(eventFactory, bufSize, threadFactory, ProducerType.MULTI, waitStrategy);
        this.disruptor.setDefaultExceptionHandler(new LoggingExceptionHandler());
        if (numWorkers == 1) {
            this.disruptor.handleEventsWith(new TaskHandler());
        } else {
            final TaskHandler[] handlers = new TaskHandler[numWorkers];
            for (int i = 0; i < numWorkers; i++) {
                handlers[i] = new TaskHandler();
            }
            this.disruptor.handleEventsWithWorkerPool(handlers);
        }
        this.disruptor.start();
    }

    @Override
    public boolean dispatch(final Runnable message) {
        final RingBuffer<MessageEvent<Runnable>> ringBuffer = disruptor.getRingBuffer();
        try {
            final long sequence = ringBuffer.tryNext();
            try {
                final MessageEvent<Runnable> event = ringBuffer.get(sequence);
                event.setMessage(message);
            } finally {
                ringBuffer.publish(sequence);
            }
            return true;
        } catch (final InsufficientCapacityException e) {
            // This exception is used by the Disruptor as a global goto. It is a singleton
            // and has no stack trace.  Don't worry about performance.
            return false;
        }
    }

    @Override
    public void execute(final Runnable message) {
        if (!dispatch(message)) {
            message.run(); // If fail to dispatch, caller run.
        }
    }

    @Override
    public void shutdown() {
        this.disruptor.shutdown();
    }
}
