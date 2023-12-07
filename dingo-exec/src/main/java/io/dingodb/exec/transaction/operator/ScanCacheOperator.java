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

package io.dingodb.exec.transaction.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.fin.OperatorProfile;
import io.dingodb.exec.operator.IteratorSourceOperator;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.impl.TransactionCache;
import io.dingodb.exec.transaction.impl.TransactionManager;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

@Slf4j
@JsonTypeName("scanCache")
public final class ScanCacheOperator extends IteratorSourceOperator {
    @Getter
    @Setter
    private TransactionCache cache;

    @JsonProperty("schema")
    @Getter
    protected final DingoType schema;

    @JsonCreator
    public ScanCacheOperator(@JsonProperty("schema") DingoType schema) {
        super();
        this.schema = schema;
    }

    public ScanCacheOperator(TransactionCache cache, DingoType schema) {
        super();
        this.cache = cache;
        this.schema = schema;
    }

    @Override
    public boolean push() {
        long count = 0;
        long startTime = System.currentTimeMillis();
        OperatorProfile profile = getProfile();
        profile.setStartTimeStamp(startTime);
        Iterator<Object[]> iterator = createIterator();
        while (iterator.hasNext()) {
            Object[] tuple = iterator.next();
            ++count;
            if (!output.push(tuple)) {
                break;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("ScanCache push,  count: {}, cost: {}ms.", count,
                System.currentTimeMillis() - startTime);
        }
        profile.setProcessedTupleCount(count);
        profile.setEndTimeStamp(System.currentTimeMillis());
        return false;
    }

    @Override
    protected @NonNull Iterator<Object[]> createIterator() {
        Iterator<Object[]> iterator = cache.iterator();
        return iterator;
    }

    @Override
    public void init() {
        super.init();
        // cross node transaction
        if (cache == null) {
            CommonId txnId = getTask().getTxnId();
            ITransaction transaction = TransactionManager.getTransaction(txnId);
            cache = transaction.getCache();
        }
    }
}
