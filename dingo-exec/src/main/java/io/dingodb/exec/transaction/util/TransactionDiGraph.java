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

package io.dingodb.exec.transaction.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

/**
 * Keeps the DAG of wait-for-lock
 * @param <T>
 */
public class TransactionDiGraph<T> {

    private final HashMap<T, ArrayList<T>> edges;

    public TransactionDiGraph() {
        this.edges = new HashMap<>();
    }

    public void addDiEdge(T from, T to) {
        assert from != null && to != null;
        this.edges.computeIfAbsent(from, (ignored) -> new ArrayList<>()).add(to);
    }

    private static class Detector<T> {
        private final HashMap<T, ArrayList<T>> edges;
        private final HashSet<T> discovered;
        private final HashSet<T> finished;
        private ArrayList<T> curPath;

        Detector(HashMap<T, ArrayList<T>> edges) {
            this.edges = edges;
            this.discovered = new HashSet<>();
            this.finished = new HashSet<>();
        }

        private Optional<ArrayList<T>> detect() {
            for (T u : edges.keySet()) {
                if (!discovered.contains(u) && !finished.contains(u)) {
                    curPath = new ArrayList<>(1);
                    curPath.add(u);
                    Optional<ArrayList<T>> result = dfs(u);
                    if (result.isPresent()) {
                        return result;
                    }
                }
            }
            return Optional.empty();
        }

        private Optional<ArrayList<T>> dfs(T u) {
            discovered.add(u);
            for (T v : Optional.ofNullable(edges.get(u)).orElse(new ArrayList<>())) {
                if (discovered.contains(v)) {
                    return Optional.of(new ArrayList<>(curPath));
                }
                if (!finished.contains(v)) {
                    curPath.add(v);
                    Optional<ArrayList<T>> result = dfs(v);
                    curPath.remove(curPath.size() - 1);
                    if (result.isPresent()) {
                        return result;
                    }
                }
            }
            discovered.remove(u);
            finished.add(u);
            return Optional.empty();
        }
    }

    /**
     * Detect cycle in graph.
     *
     * @return any cycle, or empty if no cycle detected.
     */
    public Optional<ArrayList<T>> detect() {
        return (new Detector<T>(this.edges).detect());
    }
}
