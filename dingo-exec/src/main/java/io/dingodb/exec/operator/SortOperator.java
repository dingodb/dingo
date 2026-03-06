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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.memory.MemoryRevoker;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.data.SortCollation;
import io.dingodb.exec.operator.params.AbstractParams;
import io.dingodb.exec.operator.params.HashJoinParam;
import io.dingodb.exec.operator.params.SortParam;
import io.dingodb.tool.api.MemoryAllocatorCtx;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SortOperator extends SoleOutOperator implements MemoryRevoker {
    public static final SortOperator INSTANCE = new SortOperator();

    private SortOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            SortParam param = vertex.getParam();
            param.setContext(context);
            int limit = param.getLimit();
            int offset = param.getOffset();
            List<SortCollation> collations = param.getCollations();
            if (limit == 0) {
                return false;
            }
            param.getCache().add(tuple);
            return !collations.isEmpty() || limit < 0 || param.getCache().size() < offset + limit;
        }
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        if (fin instanceof FinWithException) {
            vertex.getSoleEdge().fin(fin);
            return;
        }
        synchronized (vertex) {
            SortParam param = vertex.getParam();
            OperatorProfile profile = param.getProfile();
            profile.start();
            int limit = param.getLimit();
            int offset = param.getOffset();
            List<Object[]> cache = param.getCache();
            int size = cache.size();
            profile.setCount(size);
            Comparator<Object[]> comparator = param.getComparator();
            if (comparator != null) {
                cache.sort(comparator);
            }
            List<Object[]> normalCache = cache;
            if (param.isVectorHybrid()) {
                // similarity score normalization
                normalCache = new ArrayList<>(size);
                List<Float> similarityScores = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    Object[] objects = cache.get(i);
                    similarityScores.add((Float) objects[1]);

                }
                List<Float> floats = normalizeScores(similarityScores);
                for (int i = 0; i < size; i++) {
                    Object[] objects = new Object[2];
                    objects[0] = cache.get(i)[0];
                    objects[1] = floats.get(i);
                    normalCache.add(objects);
                }
            }
            profile.end();
            int o = 0;
            int c = 0;
            Edge edge = vertex.getSoleEdge();
            for (Object[] tuple : normalCache) {
                if (o < offset) {
                    ++o;
                    continue;
                }
                if (limit >= 0 && c >= limit) {
                    break;
                }
                if (!edge.transformToNext(param.getContext(), tuple)) {
                    break;
                }
                ++c;
            }
            if (fin instanceof FinWithProfiles) {
                FinWithProfiles finWithProfiles = (FinWithProfiles) fin;
                finWithProfiles.addProfile(profile);
            }
            edge.fin(fin);
            // Reset
            param.clear();
        }
    }

    public static List<Float> normalizeScoresOld(List<Float> scores) {
        List<Float> validScores = scores.stream()
            .filter(score -> score != null && score >= 0)
            .collect(Collectors.toList());

        if (validScores.isEmpty()) {
            return  Collections.emptyList();
        }

        Float min = validScores.stream().min(Float::compare).orElse(0.0F);
        Float max = validScores.stream().max(Float::compare).orElse(1.0F);

        return validScores.stream()
            .map(score -> (max == min) ? 0.0F : 1 - ((score - min) / (max - min)))
            .collect(Collectors.toList());
    }

    public static List<Float> normalizeScores(List<Float> scores) {
        if (scores == null || scores.isEmpty()) {
            return Collections.emptyList();
        }

        // Find the minimum and maximum values
        Float min = scores.stream().min(Float::compare).orElse(0.0F);
        Float max = scores.stream().max(Float::compare).orElse(0.0F);

        // If the minimum and maximum values are the same, return a list of all zeros
        if (min.equals(max)) {
            return scores.stream()
                .map(score -> 0.0F)
                .collect(Collectors.toList());
        }

        // Shift and normalize the scores
        return scores.stream()
            .map(score -> (score - min) / (max - min))
            .collect(Collectors.toList());
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke(AbstractParams param) {
        SortParam sortParam = (SortParam) param;
        sortParam.addSpillCnt(1);
        return spillToDisk(sortParam);
    }

    @Override
    public void finishMemoryRevoke(AbstractParams param) {
        // finish -> releaseMemory
        SortParam sortParam = (SortParam) param;
        MemoryAllocatorCtx memoryAllocatorCtx = sortParam.getMemoryAllocatorCtx();
        memoryAllocatorCtx.releaseRevocableMemory(memoryAllocatorCtx.getRevocableAllocated(), true);
        LogUtils.info(log, "finish memory revoke, release revocable memory");
    }

    @Override
    public MemoryAllocatorCtx getMemoryAllocatorCtx(AbstractParams param) {
        SortParam sortParam = (SortParam) param;
        return sortParam.getMemoryAllocatorCtx();
    }

    public ListenableFuture<?> spillToDisk(AbstractParams param) {
        LogUtils.info(log, "start spill to disk");
        SettableFuture<?> future = SettableFuture.create();
        new Thread(() -> {
            Utils.sleep(10000);
            future.set(null);
        }).start();
        return future;
    }
}
