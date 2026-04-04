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

package io.dingodb.exec.operator.spill;

import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.exec.operator.data.SortCollation;
import io.dingodb.exec.operator.data.SortDirection;
import io.dingodb.exec.operator.data.SortNullDirection;
import io.dingodb.exec.operator.params.SortParam;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the spill-to-disk infrastructure:
 * <ul>
 *   <li>{@link SpillManager} – file lifecycle</li>
 *   <li>{@link TupleSpillFile} – write / lazy-read round-trip</li>
 *   <li>{@link SortParam} – spill batch accumulation</li>
 * </ul>
 */
public class TestSpillOperator {

    private static final DingoType INT_STRING_DOUBLE =
        DingoTypeFactory.INSTANCE.tuple("INT", "STRING", "DOUBLE");

    @AfterEach
    public void cleanupSpills() {
        SpillManager.INSTANCE.cleanupAll();
    }

    // -------------------------------------------------------------------------
    // SpillManager tests
    // -------------------------------------------------------------------------

    @Test
    public void testSpillManagerCreatesAndDeletesFiles() throws IOException {
        int before = SpillManager.INSTANCE.getActiveFileCount();
        File f = SpillManager.INSTANCE.createSpillFile();
        assertThat(f).exists();
        assertThat(SpillManager.INSTANCE.getActiveFileCount()).isEqualTo(before + 1);

        SpillManager.INSTANCE.deleteSpillFile(f);
        assertThat(f).doesNotExist();
        assertThat(SpillManager.INSTANCE.getActiveFileCount()).isEqualTo(before);
    }

    @Test
    public void testSpillManagerDeleteNullIsNoOp() {
        // Should not throw
        SpillManager.INSTANCE.deleteSpillFile(null);
    }

    // -------------------------------------------------------------------------
    // TupleSpillFile tests
    // -------------------------------------------------------------------------

    @Test
    public void testTupleSpillFileRoundTrip() throws IOException {
        List<Object[]> tuples = Arrays.asList(
            new Object[]{1, "Alice", 3.5},
            new Object[]{2, "Betty", 3.6},
            new Object[]{3, "Cindy", 3.7}
        );

        TupleSpillFile sf = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile(), INT_STRING_DOUBLE
        );
        sf.write(tuples);
        sf.finishWrite();

        assertThat(sf.getTupleCount()).isEqualTo(3);

        List<Object[]> result = new ArrayList<>();
        Iterator<Object[]> it = sf.iterator();
        while (it.hasNext()) {
            result.add(it.next());
        }
        sf.close();

        assertThat(result).hasSize(3);
        for (int i = 0; i < tuples.size(); i++) {
            assertThat(result.get(i)).containsExactly(tuples.get(i));
        }
    }

    @Test
    public void testTupleSpillFileEmptyWrite() throws IOException {
        TupleSpillFile sf = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile(), INT_STRING_DOUBLE
        );
        sf.write(Collections.emptyList());
        sf.finishWrite();

        assertThat(sf.getTupleCount()).isEqualTo(0);

        Iterator<Object[]> it = sf.iterator();
        assertThat(it.hasNext()).isFalse();
        sf.close();
    }

    @Test
    public void testTupleSpillFileMultiBatchWrite() throws IOException {
        TupleSpillFile sf = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile(), INT_STRING_DOUBLE
        );
        // Write two batches sequentially
        sf.write(Arrays.asList(
            new Object[]{1, "A", 1.0},
            new Object[]{2, "B", 2.0}
        ));
        sf.write(Arrays.asList(
            new Object[]{3, "C", 3.0},
            new Object[]{4, "D", 4.0}
        ));
        sf.finishWrite();

        assertThat(sf.getTupleCount()).isEqualTo(4);

        List<Object[]> result = new ArrayList<>();
        sf.iterator().forEachRemaining(result::add);
        sf.close();

        assertThat(result).hasSize(4);
        assertThat((Integer) result.get(0)[0]).isEqualTo(1);
        assertThat((Integer) result.get(3)[0]).isEqualTo(4);
    }

    @Test
    public void testTupleSpillFileCloseDeletesFile() throws IOException {
        TupleSpillFile sf = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile(), INT_STRING_DOUBLE
        );
        File file = sf.getFile();
        assertThat(file).exists();
        sf.close();
        assertThat(file).doesNotExist();
    }

    @Test
    public void testTupleSpillFileCannotWriteAfterFinish() throws IOException {
        TupleSpillFile sf = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile(), INT_STRING_DOUBLE
        );
        sf.finishWrite();
        assertThatThrownBy(() -> sf.write(Collections.singletonList(new Object[]{1, "X", 0.0})))
            .isInstanceOf(IllegalStateException.class);
        sf.close();
    }

    // -------------------------------------------------------------------------
    // SortParam spill batch tests
    // -------------------------------------------------------------------------

    @Test
    public void testSortParamSpillBatch() throws IOException {
        SortParam param = new SortParam(
            Collections.emptyList(), -1, 0, false, INT_STRING_DOUBLE, 3
        );
        // Simulate init (normally called by Vertex.init())
        param.getCache().add(new Object[]{3, "C", 3.0});
        param.getCache().add(new Object[]{1, "A", 1.0});
        param.getCache().add(new Object[]{2, "B", 2.0});

        assertThat(param.isSpillEnabled()).isTrue();
        assertThat(param.hasSpillFiles()).isFalse();

        param.spillCurrentBatch();

        assertThat(param.getCache()).isEmpty();
        assertThat(param.getSpilledCount()).isEqualTo(3);
        assertThat(param.hasSpillFiles()).isTrue();
        assertThat(param.getSpillFiles()).hasSize(1);

        // Read back tuples from the spill file
        List<Object[]> spilled = new ArrayList<>();
        param.getSpillFiles().get(0).iterator().forEachRemaining(spilled::add);
        assertThat(spilled).hasSize(3);

        param.clear();
        assertThat(param.hasSpillFiles()).isFalse();
        assertThat(param.getSpilledCount()).isEqualTo(0);
    }

    @Test
    public void testSortParamSpillBatchWithComparator() throws IOException {
        // Spill with a sort order: ascending on column 0 (INT)
        SortCollation col0Asc = new SortCollation(0, SortDirection.ASCENDING, SortNullDirection.LAST);
        SortParam param = new SortParam(
            Collections.singletonList(col0Asc), -1, 0, false, INT_STRING_DOUBLE, 3
        );
        param.getCache().add(new Object[]{3, "C", 3.0});
        param.getCache().add(new Object[]{1, "A", 1.0});
        param.getCache().add(new Object[]{2, "B", 2.0});

        param.spillCurrentBatch();

        // The spill file should contain sorted tuples
        List<Object[]> spilled = new ArrayList<>();
        param.getSpillFiles().get(0).iterator().forEachRemaining(spilled::add);
        assertThat(spilled).hasSize(3);
        assertThat((Integer) spilled.get(0)[0]).isEqualTo(1);
        assertThat((Integer) spilled.get(1)[0]).isEqualTo(2);
        assertThat((Integer) spilled.get(2)[0]).isEqualTo(3);

        param.clear();
    }

    @Test
    public void testSortParamSpillDisabledWhenNoSchema() {
        SortParam param = new SortParam(
            Collections.emptyList(), -1, 0, false
        );
        assertThat(param.isSpillEnabled()).isFalse();
        assertThat(param.hasSpillFiles()).isFalse();
    }

    @Test
    public void testSortParamMultipleSpillBatches() throws IOException {
        SortParam param = new SortParam(
            Collections.emptyList(), -1, 0, false, INT_STRING_DOUBLE, 2
        );
        // First batch
        param.getCache().add(new Object[]{1, "A", 1.0});
        param.getCache().add(new Object[]{2, "B", 2.0});
        param.spillCurrentBatch();
        assertThat(param.getSpilledCount()).isEqualTo(2);

        // Second batch
        param.getCache().add(new Object[]{3, "C", 3.0});
        param.getCache().add(new Object[]{4, "D", 4.0});
        param.spillCurrentBatch();
        assertThat(param.getSpilledCount()).isEqualTo(4);

        assertThat(param.getSpillFiles()).hasSize(2);

        // Collect all spilled tuples
        List<Object[]> all = new ArrayList<>();
        for (TupleSpillFile sf : param.getSpillFiles()) {
            sf.iterator().forEachRemaining(all::add);
        }
        assertThat(all).hasSize(4);

        param.clear();
    }
}
