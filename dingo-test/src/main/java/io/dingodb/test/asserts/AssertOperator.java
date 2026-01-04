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

package io.dingodb.test.asserts;

import io.dingodb.common.CommonId;
import io.dingodb.exec.OperatorFactory;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.CalcDistributionOperator;
import io.dingodb.exec.operator.PartRangeScanOperator;
import io.dingodb.exec.operator.SoleOutOperator;
import lombok.Getter;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

public final class AssertOperator extends Assert<Operator, AssertOperator> {
    @Getter
    private final Vertex vertex;

    AssertOperator(Operator obj) {
        this(obj, null);
    }

    AssertOperator(Operator obj, Vertex vertex) {
        super(obj);
        this.vertex = vertex;
    }

    public AssertOperator outputNum(int num) {
        assertThat(vertex.getOutList()).size().isEqualTo(num);
        return this;
    }

    @Nonnull
    public AssertOperator soleOutput() {
        isA(SoleOutOperator.class);
        Vertex next = vertex.getSoleEdge().getNext();
        return operator(
            next != null ? OperatorFactory.getInstance(next.getOp()) : null,
            next != null ? vertex.getTask().getVertex(next.getId()) : null);
    }

    public AssertOperator isPartRangeScan(CommonId tableId, CommonId partId) {
        return isA(PartRangeScanOperator.class)
            /*.prop("tableId", tableId)
            .prop("partId", partId)*/;
    }

    public AssertOperator isCalcDistribution() {
        return isA(CalcDistributionOperator.class);
    }
}
