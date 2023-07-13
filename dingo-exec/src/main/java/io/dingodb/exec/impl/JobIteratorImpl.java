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

package io.dingodb.exec.impl;

import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobIterator;
import io.dingodb.exec.operator.RootOperator;
import org.checkerframework.checker.nullness.qual.NonNull;

public class JobIteratorImpl extends JobIterator {
    private final RootOperator operator;

    private transient Object[] current;

    JobIteratorImpl(Job job, @NonNull RootOperator operator) {
        super(job);
        this.operator = operator;
        current = operator.popValue();
    }

    @Override
    public boolean hasNext() {
        if (current != RootOperator.FIN) {
            return true;
        }
        operator.checkError();
        return false;
    }

    @Override
    public Object[] next() {
        Object[] result = current;
        current = operator.popValue();
        return result;
    }
}
