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

import io.dingodb.common.mysql.error.ErrorMessage;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.impl.JobManagerImpl;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static io.dingodb.common.mysql.error.ErrorCode.ErrRecursiveCteErr;

public class DingoEnumerable implements Iterator {
    private static final Object DUMMY = new Object();
    private Object current = (Object) DUMMY;
    private boolean seedProcessed = false;
    private int currentIteration = 0;
    int iterationLimit;
    boolean all;
    private final Iterator seedEnumerator;
    private Iterator iterativeEnumerator = null;

    private Job iterationJob;

    public DingoEnumerable(Iterator seed, Job iterationJob, boolean all, int iterationLimit) {
        this.seedEnumerator = seed;
        this.iterationJob = iterationJob;
        this.all = all;
        this.iterationLimit = iterationLimit;
    }

    @Override
    public boolean hasNext() {
        // if we are not done with the seed moveNext on it
        while (!seedProcessed) {
            if (seedEnumerator.hasNext()) {
                Object value = seedEnumerator.next();
                if (checkValue(value)) {
                    current = value;
                    return true;
                }
            } else {
                seedProcessed = true;
            }
        }

        // if we are done with the seed, moveNext on the iterative part
        while (true) {
            if (iterationLimit >= 0 && currentIteration == iterationLimit) {
                String errFormat = ErrorMessage.errorMap.get(ErrRecursiveCteErr);
                String error = String.format(errFormat, iterationLimit + 1);
                throw new RuntimeException(error);
                // max number of iterations reached, we are done
                //current = DUMMY;
                //return false;
            }

            Iterator iterativeEnumerator = this.iterativeEnumerator;
            if (iterativeEnumerator == null) {
                this.iterativeEnumerator = iterativeEnumerator = iterator(iterationJob);
            }

            while (iterativeEnumerator.hasNext()) {
                Object value = iterativeEnumerator.next();
                if (checkValue(value)) {
                    current = value;
                    return true;
                }
            }

            if (current == DUMMY) {
                // current iteration did not return any value, we are done
                return false;
            }

            // current iteration level (which returned some values) is finished, go to next one
            current = DUMMY;
            //iterativeEnumerator.close();
            this.iterativeEnumerator = null;
            currentIteration++;
        }
    }

    private boolean checkValue(Object value) {
        if (all) {
            return true; // no need to check duplicates
        }

        // check duplicates
//        final Wrapped wrapped = wrapper.apply(value);
//        if (!processed.contains(wrapped)) {
//            processed.add(wrapped);
//            return true;
//        }

        return false;
    }

    @Override
    public Object next() {
        if (current == DUMMY) {
            throw new NoSuchElementException();
        }
        return current;
    }

    public Iterator iterator(Job job) {
        return JobManagerImpl.INSTANCE.createIterator(job, null);
    }
}
