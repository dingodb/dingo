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

package io.dingodb.common.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.dingodb.common.util.Utils;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;

public final class TupleMapping {
    @Nonnull
    @JsonValue
    @Getter
    private final int[] mappings;

    private TupleMapping(@Nonnull int[] mappings) {
        this.mappings = mappings;
    }

    @JsonCreator
    @Nonnull
    public static TupleMapping of(int[] mappings) {
        return new TupleMapping(mappings);
    }

    @Nonnull
    public static TupleMapping of(@Nonnull List<Integer> intList) {
        int[] intArray = new int[intList.size()];
        int i = 0;
        for (Integer integer : intList) {
            intArray[i] = integer;
            ++i;
        }
        return new TupleMapping(intArray);
    }

    public int size() {
        return mappings.length;
    }

    public int get(int index) {
        return mappings[index];
    }

    @Nonnull
    public IntStream stream() {
        return Arrays.stream(mappings);
    }

    @Nonnull
    public TupleMapping reverse(int num) {
        if (num < 0) {
            num = Utils.max(mappings);
        }
        int[] result = new int[num];
        Arrays.fill(result, -1);
        for (int i = 0; i < mappings.length; ++i) {
            result[mappings[i]] = i;
        }
        return new TupleMapping(result);
    }

    public boolean contains(int index) {
        for (int i : mappings) {
            if (i == index) {
                return true;
            }
        }
        return false;
    }

    @Nonnull
    public TupleMapping inverse(int num) {
        int[] result = new int[num - mappings.length];
        int j = 0;
        for (int i = 0; i < num; ++i) {
            if (contains(i)) {
                continue;
            }
            result[j++] = i;
        }
        return new TupleMapping(result);
    }

    public <T> void map(@Nonnull T[] dst, @Nonnull T[] src) {
        for (int i = 0; i < mappings.length; i++) {
            dst[mappings[i]] = src[i];
        }
    }

    public <T> void revMap(@Nonnull T[] dst, @Nonnull T[] src) {
        for (int i = 0; i < mappings.length; i++) {
            dst[i] = src[mappings[i]];
        }
    }

    @Nonnull
    public Object[] revMap(Object[] src) {
        Object[] dst = new Object[mappings.length];
        revMap(dst, src);
        return dst;
    }
}
