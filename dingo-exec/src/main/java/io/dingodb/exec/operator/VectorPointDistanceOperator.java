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

package io.dingodb.exec.operator;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.ObjectSizeUtils;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Pair;
import io.dingodb.common.vector.VectorCalcDistance;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.TaskStatus;
import io.dingodb.exec.memory.MemoryRevoker;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.AbstractParams;
import io.dingodb.exec.operator.params.VectorPointDistanceParam;
import io.dingodb.tool.api.MemoryAllocatorCtx;
import io.dingodb.tool.api.ToolService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.exec.transaction.util.BinaryVectorUtils.getBinaryVectorList;

@Slf4j
public class VectorPointDistanceOperator extends SoleOutOperator implements MemoryRevoker {

    public static final VectorPointDistanceOperator INSTANCE = new VectorPointDistanceOperator();

    public VectorPointDistanceOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        VectorPointDistanceParam param = vertex.getParam();
        param.setContext(context);
        param.getCache().add(tuple);
        // Track memory usage for the revocation scheduler
        if (param.getMemoryAllocatorCtx() != null) {
            long tupleSize = ObjectSizeUtils.calculateSize(tuple);
            param.getMemoryAllocatorCtx().allocateRevocableMemory(tupleSize);
        }
        // Spill to disk when the in-memory buffer reaches the configured threshold,
        // or when the memory-revoking scheduler has requested it
        boolean shouldSpill = param.isSpillEnabled()
            && (param.getCache().size() >= param.getEffectiveSpillThreshold()
                || (param.getMemoryAllocatorCtx() != null
                    && param.getMemoryAllocatorCtx().isMemoryRevokingRequested()));
        if (shouldSpill) {
            try {
                param.spillCurrentBatch();
                if (param.getMemoryAllocatorCtx() != null) {
                    param.getMemoryAllocatorCtx().releaseRevocableMemory(
                        param.getMemoryAllocatorCtx().getRevocableAllocated(), true);
                    param.getMemoryAllocatorCtx().resetMemoryRevokingRequested();
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to spill VectorPointDistance buffer to disk", e);
            }
        }
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        VectorPointDistanceParam param = vertex.getParam();
        synchronized (param) {
            Edge edge = vertex.getSoleEdge();
            if (fin instanceof FinWithException) {
                edge.fin(fin);
                return;
            }
            OperatorProfile profile = param.getProfile("vectorPointDistance");
            long start = System.currentTimeMillis();
            TupleMapping selection = param.getSelection();
            // Load all tuples: in-memory cache plus any spilled to disk
            List<Object[]> cache;
            try {
                cache = param.getAllTuples();
            } catch (IOException e) {
                LogUtils.error(log, "Failed to load spilled VectorPointDistance tuples: {}", e.getMessage(), e);
                TaskStatus taskStatus = new TaskStatus();
                taskStatus.setStatus(false);
                taskStatus.setTaskId(vertex.getTask().getId().toString());
                taskStatus.setErrorMsg(e.getMessage());
                edge.fin(FinWithException.of(taskStatus));
                return;
            }
            if (!param.isBinaryVector()) {
                List<List<Float>> rightList = cache.stream().map(e ->
                    (List<Float>) e[param.getVectorIndex()]
                ).collect(Collectors.toList());
                int topn = param.getTopk();
                if (rightList.isEmpty()) {
                    edge.fin(fin);
                    return;
                }
                List<Float> floatArray = new ArrayList<>();
                List<List<List<Float>>> partition = Lists.partition(rightList, 1024);
                for (List<List<Float>> right : partition) {
                    VectorCalcDistance vectorCalcDistance = VectorCalcDistance.builder()
                        .topN(topn)
                        .isBinaryVector(false)
                        .leftList(Collections.singletonList(param.getTargetVector()))
                        .rightList(right)
                        .dimension(param.getDimension())
                        .algorithmType(param.getAlgType())
                        .metricType(param.getMetricType())
                        .build();
                    floatArray.addAll(ToolService.getDefault().vectorCalcDistance(
                        param.getRangeDistribution().getId(),
                        vectorCalcDistance).get(0));
                }
                List<Pair<Float, Object[]>> pairList = new ArrayList<>();
                for (int i = 0; i < cache.size(); i ++) {
                    Object[] tuple = cache.get(i);
                    Object[] result = Arrays.copyOf(tuple, tuple.length + 1);
                    result[tuple.length] = floatArray.get(i);
                    pairList.add(new Pair<>((Float) result[tuple.length], result));
                }
                Collections.sort(pairList, Comparator.comparing(p -> (Float)p.getKey()));
                int count = 0;
                Object[] value;

                for (Pair<Float, Object[]> pair : pairList) {
                    if (count < topn) {
                        value = pair.getValue();
                        edge.transformToNext(param.getContext(), selection.revMap(value));
                    }
                    count++;
                }
            } else {
                List<byte[]> rightList = cache.stream()
                    .map(e -> (byte[])e[param.getVectorIndex()])
                    .collect(Collectors.toList());
                int topn = param.getTopk();
                if (rightList.isEmpty()) {
                    edge.fin(fin);
                    return;
                }
                List<Float> floatArray = new ArrayList<>();
                List<byte[]> leftBinaryValues = getBinaryVectorList(param.getBinaryVector(), param.getDimension());
                for (byte[] right : rightList) {
                    List<byte[]> rightBinaryValues = getBinaryVectorList(right, param.getDimension());
                    VectorCalcDistance vectorCalcDistance = VectorCalcDistance.builder()
                        .topN(topn)
                        .isBinaryVector(true)
                        .leftBinaryValues(leftBinaryValues)
                        .rightBinaryValues(rightBinaryValues)
                        .dimension(param.getDimension())
                        .algorithmType(param.getAlgType())
                        .metricType(param.getMetricType())
                        .build();
                    floatArray.addAll(ToolService.getDefault().vectorCalcDistance(
                        param.getRangeDistribution().getId(),
                        vectorCalcDistance).get(0));
                }
                List<Pair<Float, Object[]>> pairList = new ArrayList<>();
                for (int i = 0; i < cache.size(); i ++) {
                    Object[] tuple = cache.get(i);
                    Object[] result = Arrays.copyOf(tuple, tuple.length + 1);
                    result[tuple.length] = floatArray.get(i);
                    pairList.add(new Pair<>((Float) result[tuple.length], result));
                }
                Collections.sort(pairList, Comparator.comparing(p -> (Float)p.getKey()));
                int count = 0;
                Object[] value;

                for (Pair<Float, Object[]> pair : pairList) {
                    if (count < topn) {
                        value = pair.getValue();
                        edge.transformToNext(param.getContext(), selection.revMap(value));
                    }
                    count++;
                }
            }

            param.clear();
            profile.time(start);
            edge.fin(fin);
        }
    }

    // -------------------------------------------------------------------------
    // MemoryRevoker interface implementation
    // -------------------------------------------------------------------------

    @Override
    public ListenableFuture<?> startMemoryRevoke(AbstractParams param) {
        VectorPointDistanceParam vpParam = (VectorPointDistanceParam) param;
        SettableFuture<?> future = SettableFuture.create();
        new Thread(() -> {
            try {
                vpParam.spillCurrentBatch();
                LogUtils.info(log, "VectorPointDistanceOperator spilled current batch during memory revocation");
                future.set(null);
            } catch (IOException e) {
                LogUtils.warn(log,
                    "VectorPointDistanceOperator failed to spill during memory revocation: {}", e.getMessage());
                future.setException(e);
            }
        }, "vectorpoint-spill-thread").start();
        return future;
    }

    @Override
    public void finishMemoryRevoke(AbstractParams param) {
        VectorPointDistanceParam vpParam = (VectorPointDistanceParam) param;
        if (vpParam.getMemoryAllocatorCtx() != null) {
            vpParam.getMemoryAllocatorCtx().releaseRevocableMemory(
                vpParam.getMemoryAllocatorCtx().getRevocableAllocated(), true);
            vpParam.getMemoryAllocatorCtx().resetMemoryRevokingRequested();
            LogUtils.info(log,
                "VectorPointDistanceOperator finished memory revoke, released revocable memory");
        }
    }

    @Override
    public MemoryAllocatorCtx getMemoryAllocatorCtx(AbstractParams param) {
        VectorPointDistanceParam vpParam = (VectorPointDistanceParam) param;
        return vpParam.getMemoryAllocatorCtx();
    }

}
