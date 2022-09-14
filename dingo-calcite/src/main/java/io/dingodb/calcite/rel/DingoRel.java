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

package io.dingodb.calcite.rel;

import io.dingodb.calcite.visitor.DingoRelVisitor;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;

import javax.annotation.Nonnull;

public interface DingoRel extends PhysicalNode {
    static DingoRel dingo(RelNode rel) {
        return (DingoRel) rel;
    }

    <T> T accept(@Nonnull DingoRelVisitor<T> visitor);
}
