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

package io.dingodb.calcite.visitor.function;

import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoVector;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.fun.vector.VectorImageFun;
import io.dingodb.exec.fun.vector.VectorTextFun;
import io.dingodb.exec.operator.PartVectorOperator;
import io.dingodb.exec.restful.VectorExtract;
import io.dingodb.meta.MetaService;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public final class DingoVectorVisitFun {

    private DingoVectorVisitFun() {
    }

    public static Collection<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, DingoVector rel
    ) {
        DingoRelOptTable relTable = rel.getTable();
        DingoTable dingoTable = relTable.unwrap(DingoTable.class);

        MetaService metaService = MetaService.root().getSubMetaService(relTable.getSchemaName());
        CommonId tableId = dingoTable.getTableId();
        TableDefinition td = dingoTable.getTableDefinition();

        NavigableMap<ComparableByteArray, RangeDistribution> ranges = metaService.getRangeDistribution(tableId);
        List<SqlNode> operandsList = rel.getOperands();

        Float[] floatArray;
        floatArray = getVectorFloats(operandsList);

        int topN = ((Number) Objects.requireNonNull(((SqlNumericLiteral) operandsList.get(3)).getValue())).intValue();

        List<Output> outputs = new ArrayList<>();

        // Get all index table distributions
        NavigableMap<ComparableByteArray, RangeDistribution> indexRangeDistribution =
            metaService.getIndexRangeDistribution(rel.getIndexTableId());

        // TODO: selection
        int rowTypeSize = rel.getRowType().getFieldList().size();
        int[] select = new int[rowTypeSize];
        for (int i = 0; i < rowTypeSize; i++) {
            select[i] = i;
        }

        // Get query additional parameters
        Map<String, Object> parameterMap = getParameterMap(operandsList);

        // Create tasks based on partitions
        for (RangeDistribution rangeDistribution : indexRangeDistribution.values()) {
            PartVectorOperator operator = new PartVectorOperator(
                tableId,
                rangeDistribution.id(),
                td.getDingoType(),
                td.getKeyMapping(),
                null,
                TupleMapping.of(select),
                td,
                ranges,
                rel.getIndexTableId(),
                rangeDistribution.id(),
                floatArray,
                topN,
                parameterMap
            );
            operator.setId(idGenerator.get());
            Task task = job.getOrCreate(currentLocation, idGenerator);
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }

        return outputs;
    }

    public static Float[] getVectorFloats(List<SqlNode> operandsList) {
        Float[] floatArray = null;
        SqlBasicCall basicCall = (SqlBasicCall) operandsList.get(2);
        if (basicCall.getOperator() instanceof SqlArrayValueConstructor) {
            List<SqlNode> operands = basicCall.getOperandList();
            floatArray = new Float[operands.size()];
            for (int i = 0; i < operands.size(); i++) {
                floatArray[i] = (
                    (Number) Objects.requireNonNull(((SqlNumericLiteral) operands.get(i)).getValue())
                ).floatValue();
            }
        } else {
            List<SqlNode> sqlNodes = basicCall.getOperandList();
            if (sqlNodes.size() < 2) {
                throw new RuntimeException("vector load param error");
            }
            List<Object> paramList = sqlNodes.stream().map(e -> {
                if (e instanceof SqlLiteral) {
                    return ((SqlLiteral)e).getValue();
                } else if (e instanceof SqlIdentifier) {
                    return ((SqlIdentifier)e).getSimple();
                } else {
                    return e.toString();
                }
            }).collect(Collectors.toList());
            if (paramList.get(1) == null || paramList.get(0) == null) {
                throw new RuntimeException("vector load param error");
            }
            String param = paramList.get(1).toString();
            if (param.contains("'")) {
                param = param.replace("'", "");
            }
            String funcName = basicCall.getOperator().getName();
            if (funcName.equalsIgnoreCase(VectorTextFun.NAME)) {
                floatArray = VectorExtract.getTxtVector(
                    basicCall.getOperator().getName(),
                    paramList.get(0).toString(),
                    param);
            } else if (funcName.equalsIgnoreCase(VectorImageFun.NAME)) {
                if (paramList.size() < 3) {
                    throw new RuntimeException("vector load param error");
                }
                Object localPath = paramList.get(2);
                if (!(localPath instanceof Boolean)) {
                    throw new RuntimeException("vector load param error");
                }
                floatArray = VectorExtract.getImgVector(
                    basicCall.getOperator().getName(),
                    paramList.get(0).toString(),
                    paramList.get(1),
                    (Boolean) paramList.get(2));
            }
        }
        if (floatArray == null) {
            throw new RuntimeException("vector load error");
        }
        return floatArray;
    }

    private static Map<String, Object> getParameterMap(List<SqlNode> operandsList) {
        Map<String, Object> parameterMap = new HashMap<>();
        if (operandsList.size() >= 5) {
            SqlNode sqlNode = operandsList.get(4);
            if (sqlNode != null && sqlNode instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) operandsList.get(4);
                if (sqlBasicCall.getOperator().getName().equals("MAP")) {
                    List<SqlNode> operandList = sqlBasicCall.getOperandList();
                    String currentName = "";
                    for (int i = 0; i < operandList.size(); i++) {
                        if ((i == 0 || i % 2 == 0) && operandList.get(i) instanceof SqlIdentifier) {
                            currentName = ((SqlIdentifier) operandList.get(i)).getSimple();
                        } else {
                            SqlNode node = operandList.get(i);
                            if (!currentName.equals("") && node instanceof SqlNumericLiteral) {
                                parameterMap.put(currentName, ((SqlNumericLiteral)node).getValue());
                            }
                        }
                    }
                }
            }
        }

        return parameterMap;
    }
}
