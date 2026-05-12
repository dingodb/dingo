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
import io.dingodb.common.memory.ObjectSizeUtils;
import io.dingodb.common.mysql.scope.ScopeVariables;
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
import io.dingodb.exec.operator.params.SortParam;
import io.dingodb.exec.operator.spill.TupleSpillFile;
import io.dingodb.tool.api.MemoryAllocatorCtx;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

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
            if (param.getSize() != null && param.getSize().get() > ScopeVariables.joinSpillSize()
                && !param.getExecutionContext().isInnerSql()) {
                param.getMemoryAllocatorCtx().allocateRevocableMemory(param.getSize().get());
                param.getSize().set(0);
            }
            long size = ObjectSizeUtils.calculateSize(tuple);
            param.getSize().addAndGet(size);
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
            Comparator<Object[]> comparator = param.getComparator();
            Edge edge = vertex.getSoleEdge();
            if (param.hasSpillFiles()) {
                emitFromExternalMerge(param, comparator, profile, edge, offset, limit);
            } else {
                int size = cache.size();
                profile.setCount(size);
                if (comparator != null) {
                    cache.sort(comparator);
                }
                List<Object[]> normalCache = cache;
                if (param.isVectorHybrid()) {
                    normalCache = normalizeSimilarityScores(normalCache);
                }
                emitTuples(normalCache.iterator(), edge, param.getContext(), offset, limit);
            }

            profile.end();
            if (fin instanceof FinWithProfiles) {
                FinWithProfiles finWithProfiles = (FinWithProfiles) fin;
                finWithProfiles.addProfile(profile);
            }
            edge.fin(fin);
            // Reset
            param.clear();
        }
    }

    private static List<Object[]> normalizeSimilarityScores(List<Object[]> cache) {
        int size = cache.size();
        List<Object[]> normalCache = new ArrayList<>(size);
        List<Float> similarityScores = new ArrayList<>(size);
        for (Object[] objects : cache) {
            similarityScores.add((Float) objects[1]);
        }
        List<Float> floats = normalizeScores(similarityScores);
        for (int i = 0; i < size; i++) {
            Object[] objects = new Object[2];
            objects[0] = cache.get(i)[0];
            objects[1] = floats.get(i);
            normalCache.add(objects);
        }
        return normalCache;
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
        synchronized (param) {
            SortParam sortParam = (SortParam) param;
            if (sortParam.isSpilling()) {
                LogUtils.info(log, "sort is spilling, wait future");
                while (sortParam.getSpillFuture() == null) {
                    LogUtils.info(log, "sort is spilling, future is null");
                    Utils.sleep(50);
                }
                return sortParam.getSpillFuture();
            }
            long revocable = sortParam.getMemoryAllocatorCtx().getRevocableAllocated();
            if (revocable > 1024 * 1024 * 4) {
                sortParam.addSpillCnt(1);
                return spillToDisk(sortParam);
            } else {
                return null;
            }
        }
    }

    @Override
    public void finishMemoryRevoke(AbstractParams param) {
        // finish -> releaseMemory
        SortParam sortParam = (SortParam) param;
        MemoryAllocatorCtx memoryAllocatorCtx = sortParam.getMemoryAllocatorCtx();
        memoryAllocatorCtx.releaseRevocableMemory(memoryAllocatorCtx.getRevocableAllocated(), true);
        LogUtils.debug(log, "sort finish memory revoke, release revocable memory");
    }

    @Override
    public MemoryAllocatorCtx getMemoryAllocatorCtx(AbstractParams param) {
        SortParam sortParam = (SortParam) param;
        return sortParam.getMemoryAllocatorCtx();
    }

    public ListenableFuture<?> spillToDisk(AbstractParams param) {
        LogUtils.info(log, "start spill to disk, param:{}", param);
        SortParam sortParam = (SortParam) param;
        sortParam.setSpilling(true);
        SettableFuture<?> future = SettableFuture.create();
        new Thread(() -> {
            try {
                sortParam.spillCurrentBatch();
            } catch (IOException e) {
                future.setException(e);
                return;
            }
            releaseSpill(param);
            future.set(null);
        }).start();
        future.addListener(() -> {
            sortParam.setSpillFuture(null);
        }, directExecutor());
        sortParam.setSpillFuture(future);
        return future;
    }

    private static void emitTuples(
        Iterator<Object[]> iterator, Edge edge, Context context, int offset, int limit
    ) {
        int o = 0;
        int c = 0;
        while (iterator.hasNext()) {
            Object[] tuple = iterator.next();
            if (o < offset) {
                ++o;
                continue;
            }
            if (limit >= 0 && c >= limit) {
                break;
            }
            if (!edge.transformToNext(context, tuple)) {
                break;
            }
            ++c;
        }
    }

    /**
     * Performs a K-way merge over all sorted runs (spilled files + remaining in-memory cache)
     * and streams merged tuples directly to the downstream edge, avoiding loading all data
     * back into memory.
     *
     * @param param      operator parameters (provides spill files and in-memory cache)
     * @param comparator the tuple comparator (may be {@code null} if no ORDER BY)
     * @param profile    operator profile for count tracking
     * @param edge       downstream edge to emit tuples to
     * @param offset     number of leading tuples to skip
     * @param limit      maximum number of tuples to emit ({@code < 0} means unlimited)
     */
    private static void emitFromExternalMerge(
        SortParam param,
        Comparator<Object[]> comparator,
        OperatorProfile profile,
        Edge edge,
        int offset,
        int limit
    ) {
        List<Object[]> cache = param.getCache();
        List<TupleSpillFile> spillFiles = param.getSpillFiles();

        // Spill any remaining in-memory tuples so we can open all runs uniformly
        if (!cache.isEmpty()) {
            try {
                param.spillCurrentBatch();
            } catch (IOException e) {
                throw new RuntimeException("Failed to spill final sort buffer to disk", e);
            }
        }

        long totalTupleCount = spillFiles.stream().mapToLong(TupleSpillFile::getTupleCount).sum();
        profile.setCount((int) Math.min(totalTupleCount, Integer.MAX_VALUE));

        if (comparator == null) {
            // 无排序 — 按文件顺序流式连接输出
            int o = 0;
            int c = 0;
            for (TupleSpillFile sf : spillFiles) {
                try {
                    Iterator<Object[]> it = sf.iterator();
                    while (it.hasNext()) {
                        Object[] tuple = it.next();
                        if (o < offset) {
                            ++o;
                            continue;
                        }
                        if (limit >= 0 && c >= limit) {
                            return;
                        }
                        if (!edge.transformToNext(param.getContext(), tuple)) {
                            return;
                        }
                        ++c;
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read spill file during merge", e);
                }
            }
            return;
        }

        // 使用最小堆的 K 路合并，流式输出
        PriorityQueue<RunEntry> heap = new PriorityQueue<>(
            Math.max(spillFiles.size(), 1),
            (a, b) -> comparator.compare(a.peek(), b.peek())
        );

        for (TupleSpillFile sf : spillFiles) {
            try {
                Iterator<Object[]> it = sf.iterator();
                if (it.hasNext()) {
                    heap.add(new RunEntry(it));
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to open spill file for merge", e);
            }
        }

        int o = 0;
        int c = 0;
        while (!heap.isEmpty()) {
            RunEntry entry = heap.poll();
            Object[] tuple = entry.poll();
            if (entry.hasNext()) {
                heap.add(entry);
            }
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

        LogUtils.debug(log, "External merge sort completed: {} sorted runs, emitted {} tuples",
            spillFiles.size(), c);
    }

    private static final class RunEntry {
        private final Iterator<Object[]> iterator;
        private Object[] current;

        RunEntry(Iterator<Object[]> iterator) {
            this.iterator = iterator;
            this.current = iterator.hasNext() ? iterator.next() : null;
        }

        Object[] peek() {
            return current;
        }

        Object[] poll() {
            Object[] result = current;
            current = iterator.hasNext() ? iterator.next() : null;
            return result;
        }

        boolean hasNext() {
            return current != null;
        }
    }

}
