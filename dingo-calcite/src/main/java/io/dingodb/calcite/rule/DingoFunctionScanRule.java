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

package io.dingodb.calcite.rule;

import io.dingodb.calcite.rel.DingoDiskAnnBuild;
import io.dingodb.calcite.rel.DingoDiskAnnCountMemory;
import io.dingodb.calcite.rel.DingoDiskAnnLoad;
import io.dingodb.calcite.rel.DingoDiskAnnReset;
import io.dingodb.calcite.rel.DingoDiskAnnStatus;
import io.dingodb.calcite.rel.DingoDocument;
import io.dingodb.calcite.rel.DingoFunctionScan;
import io.dingodb.calcite.rel.DingoHybridSearch;
import io.dingodb.calcite.rel.DingoVector;
import io.dingodb.calcite.rel.LogicalDingoDiskAnnBuild;
import io.dingodb.calcite.rel.LogicalDingoDiskAnnCountMemory;
import io.dingodb.calcite.rel.LogicalDingoDiskAnnLoad;
import io.dingodb.calcite.rel.LogicalDingoDiskAnnReset;
import io.dingodb.calcite.rel.LogicalDingoDiskAnnStatus;
import io.dingodb.calcite.rel.LogicalDingoDocument;
import io.dingodb.calcite.rel.LogicalDingoHybridSearch;
import io.dingodb.calcite.rel.LogicalDingoVector;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

@Slf4j
@Value.Enclosing
public class DingoFunctionScanRule extends ConverterRule {
    public static final ConverterRule.Config DEFAULT = ConverterRule.Config.INSTANCE
        .withConversion(
            TableFunctionScan.class,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoFunctionScanRule"
        )
        .withRuleFactory(DingoFunctionScanRule::new);

    public DingoFunctionScanRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        RelTraitSet traits = rel.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.of(rel.getTable()));

        if (rel instanceof LogicalDingoVector && !(rel instanceof DingoVector)) {
            LogicalDingoVector vector = (LogicalDingoVector) rel;
            return new DingoVector(
                vector.getCluster(),
                traits,
                vector.getCall(),
                vector.getTable(),
                vector.getOperands(),
                vector.getIndexTableId(),
                vector.getIndexTable(),
                vector.getSelection(),
                vector.getFilter(),
                vector.hints
            );
        } else if (rel instanceof LogicalDingoDocument && !(rel instanceof DingoDocument)) {
            LogicalDingoDocument document = (LogicalDingoDocument) rel;
            return new DingoDocument(
                document.getCluster(),
                traits,
                document.getCall(),
                document.getTable(),
                document.getOperands(),
                document.getIndexTableId(),
                document.getIndexTable(),
                document.getSelection(),
                document.getFilter(),
                document.hints,
                document.isDocumentScanFilter()
            );
        } else if (rel instanceof LogicalDingoHybridSearch && !(rel instanceof DingoHybridSearch)) {
            LogicalDingoHybridSearch hybridSearch = (LogicalDingoHybridSearch) rel;
            return new DingoHybridSearch(
                hybridSearch.getCluster(),
                traits,
                hybridSearch.getCall(),
                hybridSearch.getTable(),
                hybridSearch.getOperands(),
                hybridSearch.getDocumentIndexTableId(),
                hybridSearch.getDocumentIndexTable(),
                hybridSearch.getVectorIndexTableId(),
                hybridSearch.getVectorIndexTable(),
                hybridSearch.getSelection(),
                hybridSearch.getFilter(),
                hybridSearch.hints
            );
        } else if (rel instanceof LogicalDingoDiskAnnBuild && !(rel instanceof DingoDiskAnnBuild)) {
            LogicalDingoDiskAnnBuild vector = (LogicalDingoDiskAnnBuild) rel;
            return new DingoDiskAnnBuild(
                vector.getCluster(),
                traits,
                vector.getCall(),
                vector.getTable(),
                vector.getOperands(),
                vector.getIndexTableId(),
                vector.getIndexTable(),
                vector.getSelection(),
                vector.getFilter(),
                vector.getHints()
            );
        } else if (rel instanceof LogicalDingoDiskAnnLoad && !(rel instanceof DingoDiskAnnLoad)) {
            LogicalDingoDiskAnnLoad vector = (LogicalDingoDiskAnnLoad) rel;
            return new DingoDiskAnnLoad(
                vector.getCluster(),
                traits,
                vector.getCall(),
                vector.getTable(),
                vector.getOperands(),
                vector.getIndexTableId(),
                vector.getIndexTable(),
                vector.getSelection(),
                vector.getFilter(),
                vector.getHints()
            );
        } else if (rel instanceof LogicalDingoDiskAnnStatus && !(rel instanceof DingoDiskAnnStatus)) {
            LogicalDingoDiskAnnStatus vector = (LogicalDingoDiskAnnStatus) rel;
            return new DingoDiskAnnStatus(
                vector.getCluster(),
                traits,
                vector.getCall(),
                vector.getTable(),
                vector.getOperands(),
                vector.getIndexTableId(),
                vector.getIndexTable(),
                vector.getSelection(),
                vector.getFilter(),
                vector.getHints()
            );
        } else if (rel instanceof LogicalDingoDiskAnnCountMemory && !(rel instanceof DingoDiskAnnCountMemory)) {
            LogicalDingoDiskAnnCountMemory vector = (LogicalDingoDiskAnnCountMemory) rel;
            return new DingoDiskAnnCountMemory(
                vector.getCluster(),
                traits,
                vector.getCall(),
                vector.getTable(),
                vector.getOperands(),
                vector.getIndexTableId(),
                vector.getIndexTable(),
                vector.getSelection(),
                vector.getFilter(),
                vector.getHints()
            );
        } else if (rel instanceof LogicalDingoDiskAnnReset && !(rel instanceof DingoDiskAnnReset)) {
            LogicalDingoDiskAnnReset vector = (LogicalDingoDiskAnnReset) rel;
            return new DingoDiskAnnReset(
                vector.getCluster(),
                traits,
                vector.getCall(),
                vector.getTable(),
                vector.getOperands(),
                vector.getIndexTableId(),
                vector.getIndexTable(),
                vector.getSelection(),
                vector.getFilter(),
                vector.getHints()
            );
        } else if (rel instanceof DingoFunctionScan) {
            DingoFunctionScan scan = (DingoFunctionScan) rel;
            return new DingoFunctionScan(
                scan.getCluster(),
                traits,
                scan.getCall(),
                scan.getTable(),
                scan.getOperands()
            );
        }

        return null;
    }
}
