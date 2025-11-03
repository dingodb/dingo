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

import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.common.type.TupleMapping;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Getter
public class LogicalGenerateSeries extends TableFunctionScan {

    protected final RexCall call;

    protected final DingoRelOptTable table;

    protected final List<Object> operands;

    protected final TupleMapping selection;

    protected final RexNode filter;

    public LogicalGenerateSeries(RelOptCluster cluster,
                                    RelTraitSet traitSet,
                                    RexCall call,
                                    DingoRelOptTable table,
                                    List<Object> operands,
                                    TupleMapping selection,
                                    RexNode filter) {
        super(cluster, traitSet, Collections.emptyList(), call, null, call.type, null);
        this.call = call;
        this.table = table;
        this.selection = selection;
        this.operands = operands;
        this.filter = filter;
    }

    @Override
    public boolean deepEquals(@Nullable Object obj) {
        if (obj instanceof LogicalGenerateSeries) {
            LogicalGenerateSeries that = (LogicalGenerateSeries) obj;
            return super.deepEquals(obj);
        }
        return false;
    }

    @Override
    public TableFunctionScan copy(RelTraitSet traitSet,
                                  List<RelNode> inputs,
                                  RexNode rexCall,
                                  @Nullable Type elementType,
                                  RelDataType rowType,
                                  @Nullable Set<RelColumnMapping> columnMappings) {
        return new LogicalGenerateSeries(getCluster(), traitSet, call, table, operands, selection, filter);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        return pw;
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }
}
