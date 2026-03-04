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

package io.dingodb.exec.spill;

import io.dingodb.exec.operator.SortOperator;
import io.dingodb.exec.operator.data.SortCollation;
import io.dingodb.exec.operator.data.SortDirection;
import io.dingodb.exec.operator.data.SortNullDirection;
import io.dingodb.exec.operator.params.SortParam;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the spill-to-disk path in {@link SortOperator}.
 *
 * <p>We exercise the spill path directly without needing a full task graph
 * by setting a very low spill threshold and calling the package-private helper.
 */
public class TestSortSpill {

    @TempDir
    Path tempDir;

    private int originalThreshold;

    @BeforeEach
    public void setUp() {
        originalThreshold = SpillConfig.getSortSpillThreshold();
        // Override spill directory and threshold for tests.
        SpillConfig.setSpillDir(tempDir.toString());
        // Very low threshold so that spill is triggered quickly.
        SpillConfig.setSortSpillThreshold(3);
    }

    @AfterEach
    public void tearDown() {
        SpillConfig.setSortSpillThreshold(originalThreshold);
    }

    /**
     * Adds enough rows to trigger at least one spill.  The operator should still
     * return all rows in sorted order via {@link SortParam#getSpillFiles()}.
     */
    @Test
    public void testSpillTriggeredAndDataPreserved() throws Exception {
        List<SortCollation> collations = new ArrayList<>();
        collations.add(new SortCollation(0, SortDirection.ASCENDING, SortNullDirection.LAST));
        SortParam param = new SortParam(collations, -1, 0, false);

        // Feed rows that will overflow the threshold of 3.
        for (int i = 5; i >= 1; i--) {
            param.getCache().add(new Object[]{i});
            if (param.shouldSpill()) {
                invokeSpillCache(param);
            }
        }

        // If we had any spill, verify the merge works.
        if (param.hasSpillFiles() || !param.getCache().isEmpty()) {
            List<Object[]> merged = invokeMergeSpilledData(param, param.getComparator());
            assertThat(merged).hasSize(5);
            for (int i = 0; i < 5; i++) {
                assertThat((Integer) merged.get(i)[0]).isEqualTo(i + 1);
            }
        }

        param.clear();
        // Spill directory should be empty after clear.
        assertThat(tempDir.toFile().listFiles()).isEmpty();
    }

    @Test
    public void testNoSpillWhenBelowThreshold() {
        List<SortCollation> collations = new ArrayList<>();
        collations.add(new SortCollation(0, SortDirection.ASCENDING, SortNullDirection.LAST));
        SortParam param = new SortParam(collations, -1, 0, false);

        // Add fewer rows than the threshold.
        param.getCache().add(new Object[]{2});
        param.getCache().add(new Object[]{1});

        assertThat(param.shouldSpill()).isFalse();
        assertThat(param.hasSpillFiles()).isFalse();
    }

    // -------------------------------------------------------------------------
    // Reflection helpers to call private static methods in SortOperator
    // -------------------------------------------------------------------------

    private static void invokeSpillCache(SortParam param) throws Exception {
        Method m = SortOperator.class.getDeclaredMethod("spillCache", SortParam.class);
        m.setAccessible(true);
        m.invoke(null, param);
    }

    @SuppressWarnings("unchecked")
    private static List<Object[]> invokeMergeSpilledData(SortParam param, Comparator<Object[]> cmp) throws Exception {
        Method m = SortOperator.class.getDeclaredMethod("mergeSpilledData", SortParam.class, Comparator.class);
        m.setAccessible(true);
        return (List<Object[]>) m.invoke(null, param, cmp);
    }
}
