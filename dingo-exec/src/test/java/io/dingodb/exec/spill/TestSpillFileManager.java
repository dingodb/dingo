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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSpillFileManager {

    @TempDir
    Path tempDir;

    private SpillFileManager manager;

    @BeforeEach
    public void setUp() {
        manager = new SpillFileManager(tempDir.toString());
    }

    @AfterEach
    public void tearDown() {
        // Any spill files should have been cleaned up by the tests; this is a safety net.
    }

    // -------------------------------------------------------------------------
    // createSpillFile
    // -------------------------------------------------------------------------

    @Test
    public void testCreateSpillFileReturnsDistinctFiles() {
        SpillFile sf1 = manager.createSpillFile("op1");
        SpillFile sf2 = manager.createSpillFile("op1");

        assertThat(sf1.getPath()).isNotEqualTo(sf2.getPath());
        assertThat(sf1.getFileId()).isNotEqualTo(sf2.getFileId());
        assertThat(sf1.getRowCount()).isEqualTo(0L);
    }

    // -------------------------------------------------------------------------
    // write + readIterator
    // -------------------------------------------------------------------------

    @Test
    public void testWriteThenReadSingleBatch() throws IOException {
        SpillFile sf = manager.createSpillFile("op");
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{1, "Alice", 1.5});
        rows.add(new Object[]{2, "Bob", 2.5});
        rows.add(new Object[]{3, "Cindy", 3.5});

        manager.write(sf, rows);

        assertThat(sf.getRowCount()).isEqualTo(3L);

        List<Object[]> result = new ArrayList<>();
        try (SpillFileManager.SpillIterator it = manager.readIterator(sf)) {
            while (it.hasNext()) {
                result.add(it.next());
            }
        }

        assertThat(result).hasSize(3);
        assertThat(result.get(0)).containsExactly(1, "Alice", 1.5);
        assertThat(result.get(1)).containsExactly(2, "Bob", 2.5);
        assertThat(result.get(2)).containsExactly(3, "Cindy", 3.5);

        manager.delete(sf);
    }

    @Test
    public void testWriteMultipleBatchesThenRead() throws IOException {
        SpillFile sf = manager.createSpillFile("op");

        List<Object[]> batch1 = new ArrayList<>();
        batch1.add(new Object[]{1, "Alice"});
        batch1.add(new Object[]{2, "Bob"});

        List<Object[]> batch2 = new ArrayList<>();
        batch2.add(new Object[]{3, "Cindy"});
        batch2.add(new Object[]{4, "Dave"});

        manager.write(sf, batch1);
        manager.write(sf, batch2);

        assertThat(sf.getRowCount()).isEqualTo(4L);

        List<Object[]> result = manager.readAndDelete(sf);

        assertThat(result).hasSize(4);
        assertThat(result.get(0)[0]).isEqualTo(1);
        assertThat(result.get(1)[0]).isEqualTo(2);
        assertThat(result.get(2)[0]).isEqualTo(3);
        assertThat(result.get(3)[0]).isEqualTo(4);
    }

    @Test
    public void testWriteEmptyBatchIsNoOp() throws IOException {
        SpillFile sf = manager.createSpillFile("op");
        // Writing an empty list is a no-op: row count stays 0 and no file is created.
        manager.write(sf, new ArrayList<>());
        assertThat(sf.getRowCount()).isEqualTo(0L);
        assertThat(sf.getPath().toFile().exists()).isFalse();
        // Reading a non-existent spill file should throw an IOException.
        assertThatThrownBy(() -> manager.readIterator(sf))
            .isInstanceOf(IOException.class);
    }

    // -------------------------------------------------------------------------
    // delete
    // -------------------------------------------------------------------------

    @Test
    public void testDeleteRemovesFile() throws IOException {
        SpillFile sf = manager.createSpillFile("op");
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{42, "test"});
        manager.write(sf, rows);

        assertThat(sf.getPath().toFile().exists()).isTrue();

        manager.delete(sf);

        assertThat(sf.getPath().toFile().exists()).isFalse();
    }

    @Test
    public void testDeleteNonExistentFileIsNoOp() {
        SpillFile sf = manager.createSpillFile("op");
        // File was never written – delete should not throw.
        manager.delete(sf);
    }

    // -------------------------------------------------------------------------
    // readAndDelete
    // -------------------------------------------------------------------------

    @Test
    public void testReadAndDeleteCleansUpFile() throws IOException {
        SpillFile sf = manager.createSpillFile("op");
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{99});
        manager.write(sf, rows);

        List<Object[]> result = manager.readAndDelete(sf);

        assertThat(result).hasSize(1);
        assertThat(result.get(0)[0]).isEqualTo(99);
        assertThat(sf.getPath().toFile().exists()).isFalse();
    }

    // -------------------------------------------------------------------------
    // SpillConfig
    // -------------------------------------------------------------------------

    @Test
    public void testSpillConfigDefaults() {
        assertThat(SpillConfig.getSortSpillThreshold()).isEqualTo(SpillConfig.DEFAULT_SORT_SPILL_THRESHOLD);
        assertThat(SpillConfig.getAggSpillThreshold()).isEqualTo(SpillConfig.DEFAULT_AGG_SPILL_THRESHOLD);
        assertThat(SpillConfig.getJoinSpillThreshold()).isEqualTo(SpillConfig.DEFAULT_JOIN_SPILL_THRESHOLD);
    }

    @Test
    public void testSpillConfigOverride() {
        int originalSort = SpillConfig.getSortSpillThreshold();
        try {
            SpillConfig.setSortSpillThreshold(500);
            assertThat(SpillConfig.getSortSpillThreshold()).isEqualTo(500);
        } finally {
            SpillConfig.setSortSpillThreshold(originalSort);
        }
    }
}
