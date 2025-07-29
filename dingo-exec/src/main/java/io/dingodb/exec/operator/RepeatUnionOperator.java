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

package io.dingodb.exec.operator;

import io.dingodb.exec.base.Job;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.impl.JobManagerImpl;
import io.dingodb.exec.operator.params.RepeatUnionParam;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

@Slf4j
public class RepeatUnionOperator extends FilterProjectSourceOperator {

    public static RepeatUnionOperator INSTANCE = new RepeatUnionOperator();

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Vertex vertex) {
        RepeatUnionParam repeatUnionParam = vertex.getParam();
        Job seedJob = repeatUnionParam.seed;
        Job iterationJob = repeatUnionParam.iteration;

        Iterator seedIterator = JobManagerImpl.INSTANCE.createIterator(seedJob, null);

        // seedIterator
        // iteration
        return new DingoEnumerable(seedIterator, iterationJob, repeatUnionParam.all, repeatUnionParam.iterationLimit);
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        super.fin(pin, fin, vertex);
        RepeatUnionParam repeatUnionParam = vertex.getParam();
        Job seedJob = repeatUnionParam.seed;
        Job iterationJob = repeatUnionParam.iteration;
        JobManagerImpl.INSTANCE.removeJob(seedJob.getJobId());
        JobManagerImpl.INSTANCE.removeJob(iterationJob.getJobId());
    }
}
