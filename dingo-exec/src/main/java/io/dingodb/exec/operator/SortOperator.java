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

import io.dingodb.common.log.LogUtils;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.data.SortCollation;
import io.dingodb.exec.operator.params.SortParam;
import io.dingodb.exec.operator.spill.TupleSpillFile;
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

@Slf4j
public class SortOperator extends SoleOutOperator {
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
            // Spill to disk when the in-memory buffer reaches the configured threshold
            if (param.isSpillEnabled() && param.getCache().size() >= param.getEffectiveSpillThreshold()) {
                try {
                    param.spillCurrentBatch();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to spill sort buffer to disk", e);
                }
            }
            long totalCount = param.getCache().size() + param.getSpilledCount();
            return !collations.isEmpty() || limit < 0 || totalCount < (long) offset + (long) limit;
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

            // ----------------------------------------------------------------
            // Determine the tuple sequence to emit
            // ----------------------------------------------------------------
            List<Object[]> normalCache;

            if (param.hasSpillFiles()) {
                // External merge sort: there are on-disk sorted runs to merge.
                normalCache = externalMergeSort(param, comparator, profile);
            } else {
                // Pure in-memory sort (original path)
                int size = cache.size();
                profile.setCount(size);
                if (comparator != null) {
                    cache.sort(comparator);
                }
                normalCache = cache;
            }

            // ----------------------------------------------------------------
            // Optional similarity-score normalisation (vector hybrid queries)
            // ----------------------------------------------------------------
            if (param.isVectorHybrid()) {
                normalCache = normalizeSimilarityScores(normalCache);
            }

            profile.end();

            // ----------------------------------------------------------------
            // Emit with LIMIT / OFFSET
            // ----------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // External merge sort
    // -------------------------------------------------------------------------

    /**
     * Performs a K-way merge over all sorted runs (spilled files + remaining in-memory cache).
     *
     * <p>Each spill file holds a sorted run.  The remaining in-memory {@code cache} is sorted
     * and treated as an additional run.  A min-heap is used to emit tuples in global sorted
     * order without loading all data into memory at once.
     *
     * @param param      operator parameters (provides spill files and in-memory cache)
     * @param comparator the tuple comparator (may be {@code null} if no ORDER BY)
     * @param profile    operator profile for count tracking
     * @return the merged tuple list; when {@code comparator} is non-null the list is sorted
     */
    private static List<Object[]> externalMergeSort(
        SortParam param,
        Comparator<Object[]> comparator,
        OperatorProfile profile
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
            // No ordering required – just concatenate the spill files
            List<Object[]> result = new ArrayList<>((int) Math.min(totalTupleCount, Integer.MAX_VALUE));
            for (TupleSpillFile sf : spillFiles) {
                try {
                    sf.iterator().forEachRemaining(result::add);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read spill file during merge", e);
                }
            }
            return result;
        }

        // K-way merge using a min-heap
        List<Object[]> result = new ArrayList<>((int) Math.min(totalTupleCount, Integer.MAX_VALUE));
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

        while (!heap.isEmpty()) {
            RunEntry entry = heap.poll();
            result.add(entry.poll());
            if (entry.hasNext()) {
                heap.add(entry);
            }
        }

        LogUtils.debug(log, "External merge sort completed: {} sorted runs merged into {} tuples",
            spillFiles.size(), result.size());
        return result;
    }

    // -------------------------------------------------------------------------
    // Similarity score normalisation (unchanged from original)
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Internal helper: wraps an Iterator<Object[]> with a one-element lookahead
    // -------------------------------------------------------------------------

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
