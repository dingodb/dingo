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
import io.dingodb.exec.spill.SpillException;
import io.dingodb.exec.spill.SpillFile;
import io.dingodb.exec.spill.SpillFileManager;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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
            // Spill in-memory cache to disk if the threshold is reached.
            if (param.shouldSpill()) {
                spillCache(param);
            }
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

            List<Object[]> merged;
            if (param.hasSpillFiles()) {
                // External sort: spill remaining cache and merge all spill files.
                merged = mergeSpilledData(param, comparator);
            } else {
                // Pure in-memory sort – original path.
                if (comparator != null) {
                    cache.sort(comparator);
                }
                merged = cache;
            }

            List<Object[]> normalCache = merged;
            if (param.isVectorHybrid()) {
                // similarity score normalization
                int mergedSize = merged.size();
                normalCache = new ArrayList<>(mergedSize);
                List<Float> similarityScores = new ArrayList<>(mergedSize);
                for (int i = 0; i < mergedSize; i++) {
                    Object[] objects = merged.get(i);
                    similarityScores.add((Float) objects[1]);
                }
                List<Float> floats = normalizeScores(similarityScores);
                for (int i = 0; i < mergedSize; i++) {
                    Object[] objects = new Object[2];
                    objects[0] = merged.get(i)[0];
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

    // -------------------------------------------------------------------------
    // Spill helpers
    // -------------------------------------------------------------------------

    /**
     * Sorts the current in-memory cache and writes it to a new spill file,
     * then clears the in-memory cache.
     */
    private static void spillCache(SortParam param) {
        List<Object[]> cache = param.getCache();
        Comparator<Object[]> comparator = param.getComparator();
        if (comparator != null) {
            cache.sort(comparator);
        }
        SpillFileManager mgr = param.getOrCreateSpillFileManager();
        SpillFile sf = mgr.createSpillFile("sort");
        try {
            mgr.write(sf, cache);
        } catch (IOException e) {
            throw new SpillException("Failed to spill sort cache", e);
        }
        param.addSpillFile(sf);
        LogUtils.debug(log, "Spilled {} rows to {}", cache.size(), sf.getPath());
        cache.clear();
    }

    /**
     * External merge: spills the remaining in-memory cache (if non-empty), then
     * performs a sequential merge of all spill files (reading each entirely into
     * memory and merging).  The resulting list is fully sorted.
     *
     * <p>For very large datasets a full k-way merge would be preferred, but for
     * the initial implementation a simple two-pass approach is sufficient.
     */
    private static List<Object[]> mergeSpilledData(SortParam param, Comparator<Object[]> comparator) {
        // Spill remaining in-memory rows first.
        if (!param.getCache().isEmpty()) {
            spillCache(param);
        }
        SpillFileManager mgr = param.getOrCreateSpillFileManager();
        List<Object[]> merged = new ArrayList<>();
        for (SpillFile sf : param.getSpillFiles()) {
            try {
                merged.addAll(mgr.readAndDelete(sf));
            } catch (IOException e) {
                throw new SpillException("Failed to read spill file " + sf.getPath(), e);
            }
        }
        param.getSpillFiles().clear();
        if (comparator != null) {
            merged.sort(comparator);
        }
        return merged;
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

}
