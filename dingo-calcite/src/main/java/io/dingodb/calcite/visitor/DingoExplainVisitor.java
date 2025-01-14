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

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.rel.DingoDiskAnnBuild;
import io.dingodb.calcite.rel.DingoDiskAnnCountMemory;
import io.dingodb.calcite.rel.DingoDiskAnnLoad;
import io.dingodb.calcite.rel.DingoDiskAnnReset;
import io.dingodb.calcite.rel.DingoDiskAnnStatus;
import io.dingodb.calcite.rel.DingoDocument;
import io.dingodb.calcite.rel.DingoExportData;
import io.dingodb.calcite.rel.DingoFilter;
import io.dingodb.calcite.rel.DingoForUpdate;
import io.dingodb.calcite.rel.DingoFunctionScan;
import io.dingodb.calcite.rel.DingoGetByIndex;
import io.dingodb.calcite.rel.DingoGetByIndexMerge;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoGetDocumentPreFilter;
import io.dingodb.calcite.rel.DingoGetVectorByDistance;
import io.dingodb.calcite.rel.DingoHybridSearch;
import io.dingodb.calcite.rel.DingoInfoSchemaScan;
import io.dingodb.calcite.rel.DingoLikeScan;
import io.dingodb.calcite.rel.DingoPartCountDelete;
import io.dingodb.calcite.rel.DingoPartRangeDelete;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoReduce;
import io.dingodb.calcite.rel.DingoRel;
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.DingoUnion;
import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.calcite.rel.DingoVector;
import io.dingodb.calcite.rel.DocumentStreamConvertor;
import io.dingodb.calcite.rel.VectorStreamConvertor;
import io.dingodb.calcite.rel.dingo.DingoDocumentScanFilter;
import io.dingodb.calcite.rel.dingo.DingoHashJoin;
import io.dingodb.calcite.rel.dingo.DingoIndexScanWithRelOp;
import io.dingodb.calcite.rel.dingo.DingoReduceAggregate;
import io.dingodb.calcite.rel.dingo.DingoRelOp;
import io.dingodb.calcite.rel.dingo.DingoRoot;
import io.dingodb.calcite.rel.dingo.DingoScanWithRelOp;
import io.dingodb.calcite.rel.dingo.DingoSort;
import io.dingodb.calcite.rel.dingo.DingoStreamingConverter;
import io.dingodb.calcite.rel.dingo.IndexFullScan;
import io.dingodb.calcite.rel.dingo.IndexRangeScan;
import io.dingodb.common.CommonId;
import io.dingodb.common.mysql.Explain;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rel.DingoRel.dingo;

public class DingoExplainVisitor implements DingoRelVisitor<Explain> {

    @Override
    public Explain visit(@NonNull DingoAggregate rel) {
        String info = "";
        String accessObj = "";
        if (rel.getAggCallList() != null) {
            info = rel.getAggCallList().stream().map(AggregateCall::toString).collect(Collectors.joining());
        }
        return getCommonExplain(rel, "dingoAggregate", accessObj, info);
    }

    @Override
    public Explain visit(@NonNull DingoFilter rel) {
        String info = "";
        String accessObj = "";
        if (rel.getCondition() != null) {
            info = rel.getCondition().toString();
        }
        return getCommonExplain(rel, "dingoFilter", accessObj, info);
    }

    @Override
    public Explain visit(@NonNull DingoGetByIndex rel) {
        String info = "";
        if (rel.getFilter() != null) {
            info = rel.getFilter().toString();
        }
        List<CommonId> idList = rel.getIndexSetMap().keySet().stream().collect(Collectors.toList());
        Map<CommonId, Table> tableIndexes = rel.getIndexTdMap();
        List<String> nameList = idList.stream()
            .filter(id -> tableIndexes.containsKey(id))
            .map(id -> tableIndexes.get(id).getName())
            .collect(Collectors.toList());
        String table = StringUtils.join(nameList);
        return new Explain("dingoGetByIndex", rel.getRowCount(), "root", table, info);
    }

    @Override
    public Explain visit(@NonNull DingoGetByKeys rel) {
        String info = "";
        if (rel.getFilter() != null) {
            info = rel.getFilter().toString();
        }
        String table = Objects.requireNonNull(rel.getTable().unwrap(DingoTable.class)).getTable().getName();
        return new Explain("dingoGetByKeys", rel.getPoints().size(), "root", table, info);
    }

    @Override
    public Explain visit(@NonNull DingoHashJoin rel) {
        String info = "";
        if (rel.getJoinType() != null && rel.getCondition() != null) {
            info = "joinType:" + rel.getJoinType().toString();
            info += ", condition:" + rel.getCondition().toString();
        }
        Explain explain1 = new Explain("dingoHashJoin", rel.getRowCount(), "root", "", info);
        for (RelNode node : rel.getInputs()) {
            explain1.getChildren().add(dingo(node).accept(this));
        }
        return explain1;
    }

    @Override
    public Explain visit(@NonNull DingoTableModify rel) {
        String info = "";
        String accessObj = "";
        if (rel.getOperation() != null && rel.getSourceExpressionList() != null) {
            info = rel.getOperation().toString();
            info += rel.getSourceExpressionList().stream().map(RexNode::toString).collect(Collectors.joining());
        }
        if (rel.getTable() != null) {
            accessObj = Objects.requireNonNull(rel.getTable().unwrap(DingoTable.class)).getTable().getName();
        }
        return getCommonExplain(rel, "dingoTableModify", accessObj, info);
    }

    @Override
    public Explain visit(@NonNull DingoProject rel) {
        String info = "";
        if (rel.getProjects() != null) {
            info = rel.getProjects().stream().map(RexNode::toString).collect(Collectors.joining());
        }
        return getCommonExplain(rel, "dingoProject", "", info);
    }

    @Override
    public Explain visit(@NonNull DingoReduce rel) {
        String info = "";
        if (rel.getAggregateCallList() != null) {
            info = rel.getAggregateCallList().stream().map(AggregateCall::toString).collect(Collectors.joining());
        }
        return getCommonExplain(rel, "dingoReduce", "", info);
    }

    @Override
    public Explain visit(@NonNull DingoRoot rel) {
        return getCommonExplain(rel, "root-collect", "", "");
    }

    @Override
    public Explain visit(@NonNull DingoSort rel) {
        String info = "";
        if (rel.getCollation() != null) {
            if (!rel.getCollation().getFieldCollations().isEmpty()) {
                info = rel.getCollation().getFieldCollations().stream()
                    .map(fc -> {
                        StringBuilder softInfo = new StringBuilder(" order by ");
                        softInfo.append(fc.getFieldIndex()).append(" ").append(fc.getDirection().toString());
                        return softInfo.toString();
                    }).collect(Collectors.joining());
            } else {
                info = " cancel order by ";
            }
            StringBuilder infoBuilder = new StringBuilder(info);
            infoBuilder.append(",offset = ");
            infoBuilder.append(rel.offset == null ? "null" : rel.offset.toString());
            infoBuilder.append(",fetch = ");
            infoBuilder.append(rel.fetch == null ? "null" : rel.fetch.toString());
            info = infoBuilder.toString();
        }

        return getCommonExplain(rel, "dingoSort", "", info);
    }

    @Override
    public Explain visit(@NonNull DingoStreamingConverter rel) {
        Explain explain = dingo(rel.getInput()).accept(this);
        Explain explain1 = new Explain("streaming", rel.getRowCount(), "root", "", "");
        explain1.getChildren().add(explain);
        return explain1;
    }

    @Override
    public Explain visit(@NonNull DingoTableScan rel) {
        String filter = "";
        String accessObj = "";
        if (rel.getFilter() != null) {
            filter = rel.getFilter().toString();
        }
        if (rel.getTable() != null) {
            accessObj = Objects.requireNonNull(rel.getTable().unwrap(DingoTable.class)).getTable().getName();
        }
        return new Explain(
            "dingoTableScan", rel.getRowCount(), "root",
            accessObj, filter
        );
    }

    @Override
    public Explain visit(@NonNull DingoUnion rel) {
        Explain explain1 = new Explain("dingoUnion", rel.getRowCount(), "root", "", "");
        for (RelNode node : rel.getInputs()) {
            explain1.getChildren().add(dingo(node).accept(this));
        }
        return explain1;
    }

    @Override
    public Explain visit(@NonNull DingoValues rel) {
        String filter = "";
        String tableNames = "";
        return new Explain(
            "dingoValues", rel.getRowCount(), "root",
            tableNames, filter
        );
    }

    @Override
    public Explain visit(@NonNull DingoPartCountDelete rel) {
        String accessObj = "";
        if (rel.getTable() != null) {
            accessObj = Objects.requireNonNull(rel.getTable().unwrap(DingoTable.class)).getTable().getName();
        }
        return getCommonExplain(rel, "dingoPartCountDelete", accessObj, "");
    }

    @Override
    public Explain visit(@NonNull DingoPartRangeDelete rel) {
        String accessObj = "";
        if (rel.getTable() != null) {
            accessObj = Objects.requireNonNull(rel.getTable().unwrap(DingoTable.class)).getTable().getName();
        }
        return getCommonExplain(rel, "dingoPartRangeDelete", accessObj, "");
    }

    @Override
    public Explain visit(@NonNull DingoLikeScan rel) {
        String filter = "";
        String tableNames = "";
        return new Explain(
            "dingoLikeScan", rel.getRowCount(), "root",
            tableNames, filter
        );
    }

    @Override
    public Explain visit(@NonNull DingoFunctionScan rel) {
        String filter = "";
        String tableNames = "";
        return new Explain(
            "dingoFunctionScan", rel.getRowCount(), "root",
            tableNames, filter
        );
    }

    @Override
    public Explain visit(@NonNull DingoVector rel) {
        String filter = "";
        if (rel.getFilter() != null) {
            filter = rel.getFilter().toString();
        }
        String tableNames = "";
        if (rel.getIndexTable() != null) {
            tableNames = rel.getIndexTable().getName();
        }
        return new Explain(
            "dingoVector", rel.getRowCount(), "root",
            tableNames, filter
        );
    }

    @Override
    public Explain visit(@NonNull DingoDocument rel) {
        StringBuilder filter = new StringBuilder();
        if (rel.getFilter() != null) {
            filter.append("condition=").append(rel.getFilter().toString()).append(",");
        }
        filter.append("queryStr=").append(rel.getQueryStr());
        String tableNames = "";
        if (rel.getIndexTable() != null) {
            tableNames = rel.getIndexTable().getName();
        }
        return new Explain(
            "dingDocument", rel.getRowCount(), "root",
            tableNames, filter.toString()
        );
    }

    @Override
    public Explain visit(@NonNull DingoHybridSearch rel) {
        String filter = "";
        if (rel.getFilter() != null) {
            filter = rel.getFilter().toString();
        }
        String tableNames = "";
        if (rel.getDocumentIndexTable() != null) {
            tableNames = rel.getDocumentIndexTable().getName();
        }
        if (rel.getVectorIndexTable() != null) {
            tableNames = tableNames + "," + rel.getVectorIndexTable().getName();
        }
        return new Explain(
            "dingoHybridSearch", rel.getRowCount(), "root",
            tableNames, filter
        );
    }

    @Override
    public Explain visit(@NonNull DingoGetVectorByDistance rel) {
        String accessObj = "";
        if (rel.getIndexTable() != null) {
            accessObj = rel.getIndexTable().getName();
        }
        return getCommonExplain(rel, "dingoGetVectorByDistance", accessObj, "");
    }

    @Override
    public Explain visit(@NonNull VectorStreamConvertor rel) {
        String accessObj = "";
        if (rel.getIndexTableDefinition() != null) {
            accessObj = rel.getIndexTableDefinition().getName();
        }
        return getCommonExplain(rel, "vectorStreamConverter", accessObj, "");
    }

    @Override
    public Explain visit(@NonNull DocumentStreamConvertor rel) {
        String accessObj = "";
        if (rel.getIndexTableDefinition() != null) {
            accessObj = rel.getIndexTableDefinition().getName();
        }
        return getCommonExplain(rel, "documentStreamConverter", accessObj, "");
    }

    @Override
    public Explain visit(@NonNull DingoGetByIndexMerge rel) {
        String filter = "";
        if (rel.getFilter() != null) {
            filter = rel.getFilter().toString();
        }
        String tableNames = rel.getIndexTdMap().values().stream().map(Table::getName).collect(Collectors.joining());
        return new Explain(
            "dingoGetByIndexMerge", rel.getRowCount(), "root",
            tableNames, filter
        );
    }

    @Override
    public Explain visit(@NonNull DingoInfoSchemaScan rel) {
        String filter = "";
        if (rel.getFilter() != null) {
            filter = rel.getFilter().toString();
        }
        String accessObj = "";
        if (rel.getTable() != null) {
            accessObj = Objects.requireNonNull(rel.getTable().unwrap(DingoTable.class)).getTable().getName();
        }
        return new Explain("dingoInfoSchemaScan", rel.getRowCount(), "root", accessObj, filter);
    }

    @Override
    public Explain visit(@NonNull DingoExportData rel) {
        Explain explain = dingo(rel.getInput()).accept(this);
        Explain explain1 = new Explain(
            "dingoExportData", rel.getRowCount(), "root", "", "export data"
        );
        explain1.getChildren().add(explain);
        return explain1;
    }

    @Override
    public Explain visit(@NonNull DingoForUpdate rel) {
        String filter = "";
        String tableNames = "";
        return new Explain(
            "dingoForUpdate", rel.getRowCount(), "root",
            tableNames, filter
        );
    }

    @Override
    public Explain visit(@NonNull IndexFullScan rel) {
        StringBuilder info = new StringBuilder();
        if (rel.getFilter() != null) {
            info.append("condition:");
            info.append(rel.getFilter().toString());
        }
        info.append(",lookup:").append(rel.isLookup());
        if (rel.getSelection() != null && rel.getSelection().size() < 10) {
            info.append(",selection:").append(rel.getSelection().toString());
        }
        StringBuilder rootInfo = new StringBuilder("rpcBatchSize:" + ScopeVariables.getRpcBatchSize());
        rootInfo.append(", parallel = ").append(Utils.parallel(rel.getKeepSerialOrder())).append(" ");
        Explain explain1 = new Explain(
            "indexFullScanReader", rel.getRowCount(), "root",
            "", rootInfo.toString()
        );
        if (rel.isPushDown() && rel.getFilter() != null) {
            Explain explain = new Explain("indexFullScan", rel.getFullRowCount(),
                "cop[store]", rel.getIndexTable().getName(), info.toString());
            explain1.getChildren().add(explain);
        }
        return explain1;
    }

    @Override
    public Explain visit(@NonNull IndexRangeScan indexRangeScan) {
        StringBuilder filter = new StringBuilder();
        filter.append("parallel=").append(Utils.parallel(indexRangeScan.getKeepSerialOrder()));
        if (indexRangeScan.getFilter() != null) {
            filter.append(", condition=").append(indexRangeScan.getFilter().toString());
        }
        filter.append(", lookup:").append(indexRangeScan.isLookup());
        return new Explain(
            "indexRangeScan", indexRangeScan.getRowCount(), "root",
            indexRangeScan.getIndexTable().getName(), filter.toString()
        );
    }

    @Override
    public Explain visit(@NonNull DingoGetDocumentPreFilter rel) {
        String accessObj = "";
        if (rel.getIndexTable() != null) {
            accessObj = rel.getIndexTable().getName();
        }
        return getCommonExplain(rel, "DingoGetDocumentPreFilter", accessObj, "");
    }

    private Explain getCommonExplain(DingoRel rel, String id, String accessObj, String info) {
        Explain explain = dingo(rel.getInput(0)).accept(this);
        Explain explain1 = new Explain(
            id, rel.getRowCount(), "root", accessObj, info
        );
        explain1.getChildren().add(explain);
        return explain1;
    }

    @Override
    public Explain visitDingoRelOp(@NonNull DingoRelOp rel) {
        Explain explain = dingo(rel.getInput()).accept(this);
        String info = "";
        if (rel.getRelOp() != null) {
            info = rel.getRelOp().toString();
        }
        Explain explain1 = new Explain(
            "dingoRelOp", rel.getRowCount(), "root", "", info
        );
        explain1.getChildren().add(explain);
        return explain1;
    }

    @Override
    public Explain visitDingoScanWithRelOp(@NonNull DingoScanWithRelOp rel) {
        String table = Objects.requireNonNull(rel.getTable().unwrap(DingoTable.class)).getTable().getName();
        StringBuilder infoBuilder = new StringBuilder("parallel = " + (Utils.parallel(rel.getKeepSerialOrder())));
        if (rel.getRelOp() != null) {
            infoBuilder.append(",");
            infoBuilder.append(rel.getRelOp().toString());
        }
        Explain explain;
        if (rel.isRangeScan()) {
            explain = new Explain("tableRangeScan", rel.getRowCount(), "cop[store]", table,
                infoBuilder.toString());
        } else {
            explain = new Explain("tableFullScan", rel.getFullRowCount(), "cop[store]", table,
                infoBuilder.toString());
        }
        int limit = rel.getLimit();
        if (limit == 0) {
            limit = ScopeVariables.getRpcBatchSize();
        }
        String rootInfo = "batchSize:" + limit;
        Explain explain1 = new Explain("dingoScanRelOp", rel.getRowCount(), "root", "", rootInfo);
        explain1.getChildren().add(explain);
        return explain1;
    }

    @Override
    public Explain visitDingoAggregateReduce(@NonNull DingoReduceAggregate rel) {
        Explain explain = dingo(rel.getInput()).accept(this);
        String info = "";
        if (rel.getRelOp() != null) {
            info = rel.getRelOp().toString();
        }
        Explain explain1 = new Explain(
            "dingoReduceAggregate", rel.getRowCount(), "root", "", info
        );
        explain1.getChildren().add(explain);
        return explain1;
    }

    @Override
    public Explain visitDingoIndexScanWithRelOp(@NonNull DingoIndexScanWithRelOp rel) {
        Explain explain;
        if (rel.isRangeScan()) {
            explain = new Explain("indexRangeScan", rel.getRowCount(), "cop[store]", rel.getIndexTable().getName(),
                rel.getRelOp().toString());
        } else {
            explain = new Explain("indexFullScan", rel.getFullRowCount(), "cop[store]", rel.getIndexTable().getName(),
                rel.getRelOp().toString());
        }
        StringBuilder rootInfo = new StringBuilder("rpcBatchSize:" + ScopeVariables.getRpcBatchSize());
        rootInfo.append(", parallel = ").append(Utils.parallel(rel.getKeepSerialOrder())).append(" ");
        Explain explain1 = new Explain(
            "dingoIndexScanRelOp", rel.getRowCount(), "root", "", rootInfo.toString()
            );
        explain1.getChildren().add(explain);
        return explain1;
    }

    @Override
    public Explain visit(@NonNull DingoDiskAnnStatus dingoDiskAnnStatus) {
        String filter = "";
        if (dingoDiskAnnStatus.getFilter() != null) {
            filter = dingoDiskAnnStatus.getFilter().toString();
        }
        String tableNames = "";
        if (dingoDiskAnnStatus.getIndexTable() != null) {
            tableNames = dingoDiskAnnStatus.getIndexTable().getName();
        }
        return new Explain(
            "dingoDiskAnnStatus", dingoDiskAnnStatus.getRowCount(), "root",
            tableNames, filter
        );
    }

    @Override
    public Explain visit(@NonNull DingoDiskAnnCountMemory dingoDiskAnnCountMemory) {
        String filter = "";
        if (dingoDiskAnnCountMemory.getFilter() != null) {
            filter = dingoDiskAnnCountMemory.getFilter().toString();
        }
        String tableNames = "";
        if (dingoDiskAnnCountMemory.getIndexTable() != null) {
            tableNames = dingoDiskAnnCountMemory.getIndexTable().getName();
        }
        return new Explain(
            "dingoDiskAnnCountMemory", dingoDiskAnnCountMemory.getRowCount(), "root",
            tableNames, filter
        );
    }

    @Override
    public Explain visit(@NonNull DingoDiskAnnReset dingoDiskAnnReset) {
        String filter = "";
        if (dingoDiskAnnReset.getFilter() != null) {
            filter = dingoDiskAnnReset.getFilter().toString();
        }
        String tableNames = "";
        if (dingoDiskAnnReset.getIndexTable() != null) {
            tableNames = dingoDiskAnnReset.getIndexTable().getName();
        }
        return new Explain(
            "dingoDiskAnnReset", dingoDiskAnnReset.getRowCount(), "root",
            tableNames, filter
        );
    }

    @Override
    public Explain visit(@NonNull DingoDiskAnnBuild dingoDiskAnnBuild) {
        String filter = "";
        if (dingoDiskAnnBuild.getFilter() != null) {
            filter = dingoDiskAnnBuild.getFilter().toString();
        }
        String tableNames = "";
        if (dingoDiskAnnBuild.getIndexTable() != null) {
            tableNames = dingoDiskAnnBuild.getIndexTable().getName();
        }
        return new Explain(
            "dingoDiskAnnBuild", dingoDiskAnnBuild.getRowCount(), "root",
            tableNames, filter
        );
    }

    @Override
    public Explain visit(@NonNull DingoDiskAnnLoad dingoDiskAnnLoad) {
        String filter = "";
        if (dingoDiskAnnLoad.getFilter() != null) {
            filter = dingoDiskAnnLoad.getFilter().toString();
        }
        String tableNames = "";
        if (dingoDiskAnnLoad.getIndexTable() != null) {
            tableNames = dingoDiskAnnLoad.getIndexTable().getName();
        }
        return new Explain(
            "dingoDiskAnnLoad", dingoDiskAnnLoad.getRowCount(), "root",
            tableNames, filter
        );
    }

    @Override
    public Explain visit(@NonNull DingoDocumentScanFilter indexRangeScan) {
        StringBuilder filter = new StringBuilder();
        filter.append("parallel=").append(Utils.parallel(indexRangeScan.getKeepSerialOrder()));
        if (indexRangeScan.getFilter() != null) {
            filter.append(", condition=").append(indexRangeScan.getFilter().toString());
        }
        filter.append(", queryStr:").append(indexRangeScan.getQueryString());
        filter.append(", lookup:").append(indexRangeScan.isLookup());
        return new Explain(
            "documentScanFilter", indexRangeScan.getRowCount(), "root",
            indexRangeScan.getIndexTable().getName(), filter.toString()
        );
    }

}
