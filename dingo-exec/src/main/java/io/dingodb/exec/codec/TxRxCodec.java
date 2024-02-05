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

package io.dingodb.exec.codec;

import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.tuple.TupleId;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public interface TxRxCodec {
    void encodeTuples(OutputStream os, @NonNull List<Object[]> tuples) throws IOException;

    void encodeFin(OutputStream os, Fin fin) throws IOException;

    void encodeTupleIds(OutputStream os, List<TupleId> tupleIds) throws IOException;

    List<TupleId> decode(byte[] bytes) throws IOException;
}
