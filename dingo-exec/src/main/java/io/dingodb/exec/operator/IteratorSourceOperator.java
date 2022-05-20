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

import io.dingodb.exec.fin.OperatorProfile;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;

@Slf4j
public abstract class IteratorSourceOperator extends SourceOperator {
    protected Iterator<Object[]> iterator;

    @Override
    public boolean push() {
        long count = 0;
        OperatorProfile profile = getProfile();
        profile.setStartTimeStamp(System.currentTimeMillis());
        final long startTime = System.currentTimeMillis();
        while (iterator.hasNext()) {
            Object[] tuple = iterator.next();
            ++count;
            if (!output.push(tuple)) {
                break;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("IteratorSourceOperator push,  count: {}, cost: {}ms.", count,
                System.currentTimeMillis() - startTime);
        }
        profile.setProcessedTupleCount(count);
        profile.setEndTimeStamp(System.currentTimeMillis());
        return false;
    }
}
