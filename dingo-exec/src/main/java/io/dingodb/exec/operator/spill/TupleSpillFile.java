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

import io.dingodb.common.log.LogUtils;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.codec.AvroTupleCodec;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

@Slf4j
public class TupleSpillFile implements Closeable {
    private static final int IO_BUFFER_SIZE = 64 * 1024; // 64 KB

    @Getter
    private final File file;
    private final AvroTupleCodec codec;

    private OutputStream outputStream;
    private boolean writing;

    @Getter
    private long tupleCount;

    @Getter
    private long bytesWritten;

    /**
     * 创建一个由给定的 {@code file} 支持的新溢出文件，使用 {@code schema} 进行编码。
     *
     * @param file   备份文件（如果不存在则创建；必须可写）
     * @param schema 描述 tuple 布局的 DingoType
     * @throws IOException 如果输出流无法打开
     */
    public TupleSpillFile(File file, DingoType schema) throws IOException {
        this.file = file;
        this.codec = new AvroTupleCodec(schema);
        this.outputStream = new BufferedOutputStream(new FileOutputStream(file), IO_BUFFER_SIZE);
        this.writing = true;
        this.tupleCount = 0;
    }

    /**
     * 将一批 tuple 追加到溢出文件中。
     *
     * @param tuples 要写入的 tuple；不得为 {@code null}
     * @throws IOException           if a write error occurs
     * @throws IllegalStateException if called after {@link #finishWrite()}
     */
    public void write(List<Object[]> tuples) throws IOException {
        if (!writing) {
            throw new IllegalStateException("Spill file is not in write mode");
        }
        codec.encode(outputStream, tuples);
        tupleCount += tuples.size();
    }

    /**
     * 刷新并关闭写入流，准备文件以供读取。
     *
     * <p>该方法是幂等的；多次调用它是安全的。
     *
     * @throws IOException if closing the stream fails
     */
    public void finishWrite() throws IOException {
        if (writing && outputStream != null) {
            outputStream.flush();
            outputStream.close();
            outputStream = null;
            writing = false;
            bytesWritten = file.length();
            LogUtils.debug(log, "Spill file {} finished write, tupleCount={}, bytesWritten={}",
                file.getName(), tupleCount, bytesWritten);
        }
    }

    /**
     * 返回存储在此文件中的 tuple 的惰性、只进迭代器。
     *
     * <p>如果文件仍处于写入模式，则自动调用 {@link #finishWrite()}。
     *
     * @return 按插入顺序生成存储 tuple 的迭代器
     * @throws IOException if the file cannot be opened for reading
     */
    public Iterator<Object[]> iterator() throws IOException {
        if (writing) {
            finishWrite();
        }
        return new LazyTupleIterator(new BufferedInputStream(new FileInputStream(file), IO_BUFFER_SIZE));
    }

    /**
     * 关闭文件并删除底层临时文件。
     *
     * <p>可以安全地多次拨打。
     */
    @Override
    public void close() {
        try {
            finishWrite();
        } catch (IOException ignored) {
            // Best effort
        }
        if (bytesWritten > 0) {
            SpillManager.INSTANCE.reportBytesSpilled(bytesWritten);
        }
        SpillManager.INSTANCE.deleteSpillFile(file);
    }

    // -------------------------------------------------------------------------
    // Lazy tuple iterator
    // -------------------------------------------------------------------------

    /**
     * 一种只进迭代器，一次从 {@link InputStream} 解码一个 tuple。
     * 读取最后一条记录后，流将关闭。
     */
    private final class LazyTupleIterator implements Iterator<Object[]> {

        private final InputStream inputStream;
        private BinaryDecoder decoder;
        private Object[] lookahead;
        private boolean done;

        LazyTupleIterator(InputStream inputStream) throws IOException {
            this.inputStream = inputStream;
            this.decoder = null;
            this.done = false;
            this.lookahead = fetchNext();
        }

        @Override
        public boolean hasNext() {
            return lookahead != null;
        }

        @Override
        public Object[] next() {
            if (lookahead == null) {
                throw new NoSuchElementException("No more tuples in spill file");
            }
            Object[] result = lookahead;
            try {
                lookahead = fetchNext();
            } catch (IOException e) {
                throw new RuntimeException("Error reading spill file " + file.getAbsolutePath(), e);
            }
            return result;
        }

        private Object[] fetchNext() throws IOException {
            if (done) {
                return null;
            }
            // Update decoder state before each read
            decoder = DecoderFactory.get().directBinaryDecoder(inputStream, decoder);
            Object[] tuple;
            try {
                tuple = codec.decodeOne(decoder);
            } catch (IOException e) {
                // 清除读取失败然后重新抛出
                closeStream();
                throw e;
            }
            if (tuple == null) {
                closeStream();
                return null;
            }
            return tuple;
        }

        private void closeStream() {
            if (!done) {
                done = true;
                try {
                    inputStream.close();
                } catch (IOException ignored) {
                    // Best effort
                }
            }
        }
    }
}
