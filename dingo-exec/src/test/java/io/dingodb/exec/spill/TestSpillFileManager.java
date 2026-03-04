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

import com.google.common.collect.ImmutableList;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSpillFileManager {
    private static final DingoType TYPE = DingoTypeFactory.INSTANCE.tuple("INT", "STRING", "DOUBLE");

    private Path spillDir;
    private SpillFileManager manager;

    @BeforeEach
    public void setUp() throws IOException {
        spillDir = Files.createTempDirectory("dingo-spill-test-");
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (manager != null) {
            manager.clean();
        }
        // remove the temp directory itself (should be empty after clean())
        Files.deleteIfExists(spillDir);
    }

    @Test
    public void testWriteAndRead() throws IOException {
        manager = new SpillFileManager("task1", "op1", TYPE, spillDir);

        List<Object[]> tuples = ImmutableList.of(
            new Object[]{1, "Alice", 3.5},
            new Object[]{2, "Betty", 3.6},
            new Object[]{3, "Cindy", 3.7}
        );
        manager.write(tuples);

        Iterator<Object[]> it = manager.iterator();
        List<Object[]> result = collect(it);

        assertThat(result).hasSameSizeAs(tuples);
        for (int i = 0; i < tuples.size(); i++) {
            assertThat(result.get(i)).containsExactly(tuples.get(i));
        }
    }

    @Test
    public void testMultipleFlushes() throws IOException {
        manager = new SpillFileManager("task2", "op2", TYPE, spillDir);

        List<Object[]> batch1 = ImmutableList.of(
            new Object[]{1, "Alice", 1.1},
            new Object[]{2, "Betty", 2.2}
        );
        List<Object[]> batch2 = ImmutableList.of(
            new Object[]{3, "Cindy", 3.3}
        );

        manager.write(batch1);
        manager.flush();
        manager.write(batch2);

        assertThat(manager.getSpillFileCount()).isEqualTo(2);

        Iterator<Object[]> it = manager.iterator();
        List<Object[]> result = collect(it);

        assertThat(result).hasSize(3);
        assertThat(result.get(0)).containsExactly(1, "Alice", 1.1);
        assertThat(result.get(1)).containsExactly(2, "Betty", 2.2);
        assertThat(result.get(2)).containsExactly(3, "Cindy", 3.3);
    }

    @Test
    public void testCleanDeletesFiles() throws IOException {
        manager = new SpillFileManager("task3", "op3", TYPE, spillDir);
        manager.write(ImmutableList.of(new Object[]{1, "Alice", 1.0}));
        manager.flush();
        manager.write(ImmutableList.of(new Object[]{2, "Betty", 2.0}));

        manager.clean();

        assertThat(Files.list(spillDir).count()).isEqualTo(0);
        manager = null; // prevent double-clean in tearDown
    }

    @Test
    public void testEmptyWrite() throws IOException {
        manager = new SpillFileManager("task4", "op4", TYPE, spillDir);

        manager.write(ImmutableList.of());

        Iterator<Object[]> it = manager.iterator();
        assertThat(collect(it)).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private static @NonNull List<Object[]> collect(@NonNull Iterator<Object[]> it) {
        List<Object[]> list = new ArrayList<>();
        it.forEachRemaining(list::add);
        return list;
    }
}
