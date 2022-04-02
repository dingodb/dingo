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

package io.dingodb.exec.util;

import io.dingodb.exec.base.Id;
import io.dingodb.net.SimpleTag;
import io.dingodb.net.Tag;

import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;

public final class TagUtil {
    private TagUtil() {
    }

    @Nonnull
    public static String tag(Id jobId, Id id) {
        return jobId + ":" + id;
    }

    private static Tag getTag(@Nonnull byte[] bytes) {
        return SimpleTag.builder().tag(bytes).build();
    }

    public static Tag getTag(@Nonnull String tag) {
        return getTag(toBytes(tag));
    }

    @Nonnull
    public static byte[] toBytes(@Nonnull String tag) {
        return tag.getBytes(StandardCharsets.UTF_8);
    }

    @Nonnull
    public static String fromBytes(@Nonnull byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
