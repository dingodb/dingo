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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSpillFileManager {

    @TempDir
    Path tempDir;

    private SpillFileManager mgr;

    @BeforeEach
    void setUp() {
        // Use a fresh instance with a temp directory per test.
        mgr = SpillFileManager.getInstance();
        mgr.setSpillDir(tempDir.toString());
    }

    @Test
    void testCreateSpillFile() throws IOException {
        SpillFile sf = mgr.createSpill("testOp", 0);
        assertThat(sf).isNotNull();
        assertThat(sf.getOperatorId()).isEqualTo("testOp");
        assertThat(sf.getBucketId()).isEqualTo(0);
        assertThat(sf.getFile()).exists();
        assertThat(sf.isClosed()).isFalse();
        mgr.release(sf);
    }

    @Test
    void testWriteAndReadAllBytes() throws IOException {
        List<Object[]> tuples = Arrays.asList(
            new Object[]{1, "Alice", 1.5},
            new Object[]{2, "Bob", 2.5}
        );

        byte[] serialized = TupleSerializer.serialize(tuples);

        SpillFile sf = mgr.createSpill("testOp", 0);
        mgr.write(sf, serialized);
        mgr.closeAndFlush(sf);

        assertThat(sf.isClosed()).isTrue();
        assertThat(sf.getRecordCount()).isEqualTo(1); // one write call = one record
        assertThat(sf.getByteCount()).isEqualTo(serialized.length);

        byte[] readBack = mgr.readAllBytes(sf);
        assertThat(readBack).isEqualTo(serialized);

        List<Object[]> restored = TupleSerializer.deserialize(readBack);
        assertThat(restored).hasSize(2);
        assertThat(restored.get(0)).containsExactly(1, "Alice", 1.5);
        assertThat(restored.get(1)).containsExactly(2, "Bob", 2.5);

        mgr.release(sf);
    }

    @Test
    void testReadBeforeCloseThrows() throws IOException {
        SpillFile sf = mgr.createSpill("testOp", 0);
        assertThatThrownBy(() -> mgr.read(sf)).isInstanceOf(IllegalStateException.class);
        mgr.closeAndFlush(sf);
        mgr.release(sf);
    }

    @Test
    void testReadAllBytesBeforeCloseThrows() throws IOException {
        SpillFile sf = mgr.createSpill("testOp", 0);
        assertThatThrownBy(() -> mgr.readAllBytes(sf)).isInstanceOf(IllegalStateException.class);
        mgr.closeAndFlush(sf);
        mgr.release(sf);
    }

    @Test
    void testReleaseDeletesFile() throws IOException {
        SpillFile sf = mgr.createSpill("testOp", 0);
        mgr.write(sf, new byte[]{1, 2, 3});
        mgr.closeAndFlush(sf);
        assertThat(sf.getFile()).exists();
        mgr.release(sf);
        assertThat(sf.getFile()).doesNotExist();
    }

    @Test
    void testMultipleSpillFilesPerOperator() throws IOException {
        SpillFile sf0 = mgr.createSpill("multiOp", 0);
        SpillFile sf1 = mgr.createSpill("multiOp", 1);

        byte[] data0 = TupleSerializer.serialize(Arrays.asList(new Object[]{1, "one"}));
        byte[] data1 = TupleSerializer.serialize(Arrays.asList(new Object[]{2, "two"}));

        mgr.write(sf0, data0);
        mgr.closeAndFlush(sf0);

        mgr.write(sf1, data1);
        mgr.closeAndFlush(sf1);

        assertThat(TupleSerializer.deserialize(mgr.readAllBytes(sf0))).hasSize(1);
        assertThat(TupleSerializer.deserialize(mgr.readAllBytes(sf1))).hasSize(1);

        mgr.release(sf0);
        mgr.release(sf1);
    }

    @Test
    void testEmptyFileReturnsEmptyIterator() throws IOException {
        SpillFile sf = mgr.createSpill("testOp", 0);
        mgr.closeAndFlush(sf);

        assertThat(mgr.read(sf).hasNext()).isFalse();
        assertThat(mgr.readAllBytes(sf)).isEmpty();

        mgr.release(sf);
    }

    @Test
    void testTupleSerializerRoundTrip() throws IOException {
        List<Object[]> tuples = Arrays.asList(
            new Object[]{42, "hello", 3.14, null, true},
            new Object[]{-1, "", 0.0, null, false}
        );
        byte[] bytes = TupleSerializer.serialize(tuples);
        List<Object[]> restored = TupleSerializer.deserialize(bytes);

        assertThat(restored).hasSize(tuples.size());
        for (int i = 0; i < tuples.size(); i++) {
            assertThat(restored.get(i)).containsExactly(tuples.get(i));
        }
    }
}
