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

package io.dingodb.test.dsl;

import io.dingodb.test.dsl.builder.SqlTestCaseJavaBuilder;

public class CollectionDmlCases extends SqlTestCaseJavaBuilder {
    protected CollectionDmlCases() {
        super("Collection DML");
    }

    @Override
    protected void build() {
        table("i4k_vs0_i40_f80_mp0", file("i4k_vs0_i40_f80_mp0/create.sql"))
            .init(file("i4k_vs0_i40_f80_mp0/data.sql"), 1);

        test("Update map")
            .use("table", "i4k_vs0_i40_f80_mp0")
            .modify("i4k_vs0_i40_f80_mp0")
            .step(file("i4k_vs0_i40_f80_mp0/update.sql"), count(1))
            .step(
                file("i4k_vs0_i40_f80_mp0/select_scalar.sql"),
                csv(file("i4k_vs0_i40_f80_mp0/select_scalar_updated.csv"))
            );
    }
}
