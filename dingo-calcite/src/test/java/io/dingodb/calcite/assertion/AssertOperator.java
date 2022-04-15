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

package io.dingodb.calcite.assertion;

import io.dingodb.common.CommonId;
import io.dingodb.exec.base.Input;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.operator.PartScanOperator;
import io.dingodb.exec.operator.SoleOutOperator;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

public final class AssertOperator extends Assert<Operator, AssertOperator> {
    AssertOperator(Operator obj) {
        super(obj);
    }

    public AssertOperator outputNum(int num) {
        assertThat(instance.getOutputs()).size().isEqualTo(num);
        return this;
    }

    @Nonnull
    public AssertOperator soleOutput() {
        isA(SoleOutOperator.class);
        Input input = instance.getSoleOutput().getLink();
        return operator(input != null ? instance.getTask().getOperator(input.getOperatorId()) : null);
    }

    public AssertOperator isPartScan(CommonId tableId, Object partId) {
        return isA(PartScanOperator.class)
            .prop("tableId", tableId)
            .prop("partId", partId);
    }
}
