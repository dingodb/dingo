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

package io.dingodb.calcite.visitor;

import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.aggregate.Agg;
import io.dingodb.exec.aggregate.CountAgg;
import io.dingodb.exec.aggregate.CountAllAgg;
import io.dingodb.exec.aggregate.MaxAgg;
import io.dingodb.exec.aggregate.MinAgg;
import io.dingodb.exec.aggregate.Sum0Agg;
import io.dingodb.exec.aggregate.SumAgg;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import javax.annotation.Nonnull;

import static io.dingodb.common.util.Utils.sole;

final class AggFactory {
    private AggFactory() {
    }

    @Nonnull
    static Agg getAgg(@Nonnull SqlKind kind, @Nonnull List<Integer> args, TupleSchema schema) {
        if (args.isEmpty() && kind == SqlKind.COUNT) {
            return new CountAllAgg();
        }
        int index = sole(args);
        switch (kind) {
            case COUNT:
                return new CountAgg(index);
            case SUM:
                return new SumAgg(index, schema.get(index));
            case SUM0:
                return new Sum0Agg(index, schema.get(index));
            case MIN:
                return new MinAgg(index, schema.get(index));
            case MAX:
                return new MaxAgg(index, schema.get(index));
            default:
                break;
        }
        throw new UnsupportedOperationException("Unsupported aggregation function \"" + kind + "\".");
    }
}
