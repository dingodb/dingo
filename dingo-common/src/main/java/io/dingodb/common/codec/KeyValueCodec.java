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

package io.dingodb.common.codec;

import io.dingodb.common.store.KeyValue;

import javax.annotation.Nonnull;
import java.io.IOException;

public interface KeyValueCodec {
    public Object[] decode(@Nonnull KeyValue keyValue) throws IOException;
    public KeyValue encode(@Nonnull Object[] tuple) throws IOException;
    public byte[] encodeKey(@Nonnull Object[] keys) throws IOException;
    public Object[] mapKeyAndDecodeValue(@Nonnull Object[] keys, byte[] bytes) throws IOException;
}
