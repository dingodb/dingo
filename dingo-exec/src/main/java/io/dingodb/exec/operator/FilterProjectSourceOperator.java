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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterators;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.expr.runtime.op.logical.RtLogicalOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

public abstract class FilterProjectSourceOperator extends IteratorSourceOperator {
    @JsonProperty("schema")
    protected final DingoType schema;
    @JsonProperty("filter")
    protected SqlExpr filter;
    @JsonProperty("selection")
    protected TupleMapping selection;

    public FilterProjectSourceOperator(DingoType schema, SqlExpr filter, TupleMapping selection) {
        super();
        this.schema = schema;
        this.filter = filter;
        this.selection = selection;
    }

    @Override
    protected @NonNull Iterator<Object[]> createIterator() {
        Iterator<Object[]> iterator = createSourceIterator();
        if (selection != null) {
            iterator = Iterators.transform(iterator, selection::revMap);
        }
        if (filter != null) {
            iterator = Iterators.filter(
                iterator,
                tuple -> RtLogicalOp.test(filter.eval(tuple))
            );
        }
        return iterator;
    }

    protected abstract @NonNull Iterator<Object[]> createSourceIterator();

    @Override
    public void init() {
        super.init();
        if (filter != null) {
            filter.compileIn(schema, getParasType());
        }
    }

    @Override
    public void setParas(Object[] paras) {
        super.setParas(paras);
        if (filter != null) {
            filter.setParas(paras);
        }
    }
}
