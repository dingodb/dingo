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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptNode;

import java.util.List;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("UnusedReturnValue")
public final class AssertRelNode extends Assert<RelOptNode, AssertRelNode> {
    AssertRelNode(RelOptNode obj) {
        super(obj);
    }

    @SuppressWarnings("unused")
    public AssertRelNode typeName(String typeName) {
        assertThat(instance).hasFieldOrPropertyWithValue("relTypeName", typeName);
        return this;
    }

    public AssertRelNode convention(Convention convention) {
        assertThat(instance).hasFieldOrPropertyWithValue("convention", convention);
        return this;
    }

    @Nonnull
    public AssertRelNode singleInput() {
        List<? extends RelOptNode> inputs = instance.getInputs();
        assertThat(inputs).size().isEqualTo(1);
        return new AssertRelNode(inputs.get(0));
    }

    public AssertRelNode inputNum(int num) {
        List<? extends RelOptNode> inputs = instance.getInputs();
        assertThat(inputs).size().isEqualTo(num);
        return this;
    }
}
