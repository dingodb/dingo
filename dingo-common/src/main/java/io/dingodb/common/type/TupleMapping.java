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

package io.dingodb.common.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public final class TupleMapping {
    @JsonValue
    @Getter
    @EqualsAndHashCode.Include
    private final int[] mappings;

    private TupleMapping(int[] mappings) {
        this.mappings = mappings;
    }

    @JsonCreator
    public static @NonNull TupleMapping of(int[] mappings) {
        return new TupleMapping(mappings);
    }

    public static @NonNull TupleMapping of(@NonNull List<Integer> intList) {
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

    public @NonNull IntStream stream() {
        return Arrays.stream(mappings);
    }

    public boolean isIdentity() {
        for (int i = 0; i < mappings.length; ++i) {
            if (mappings[i] != i) {
                return false;
            }
        }
        return true;
    }

    public @NonNull TupleMapping reverse(int num) {
        if (num < 0) {
            num = Arrays.stream(mappings).max().getAsInt();
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

    public int find(int index) {
        for (int i = 0; i < mappings.length; ++i) {
            if (mappings[i] == index) {
                return mappings[i];
            }
        }
        return -1;
    }

    public int findIdx(int index) {
        for (int i = 0; i < mappings.length; ++i) {
            if (mappings[i] == index) {
                return i;
            }
        }
        return -1;
    }

    public @NonNull TupleMapping inverse(int num) {
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

    public TupleMapping add(int num) {
        int[] result = new int[mappings.length + 1];
        System.arraycopy(mappings, 0, result, 0, mappings.length);
        result[mappings.length] = num;
        return new TupleMapping(result);
    }

    public TupleMapping add(int[] nums) {
        int[] result = new int[mappings.length + nums.length];
        System.arraycopy(mappings, 0, result, 0, mappings.length);
        System.arraycopy(nums, 0, result, mappings.length, nums.length);
        return new TupleMapping(result);
    }

    public TupleMapping add(TupleMapping tupleMapping) {
        return add(tupleMapping.mappings);
    }

    public <T> void map(T[] dst, T[] src) {
        for (int i = 0; i < mappings.length; i++) {
            dst[mappings[i]] = src[i];
        }
    }

    public <T> void map(T[] dst, T[] src, Function<T, T> cloneFunc) {
        for (int i = 0; i < mappings.length; i++) {
            dst[mappings[i]] = cloneFunc.apply(src[i]);
        }
    }

    public <T> void revMap(T[] dst, T[] src) {
        for (int i = 0; i < mappings.length; i++) {
            dst[i] = src[mappings[i]];
        }
    }

    public <T> void revMap(T[] dst, T[] src, Function<T, T> cloneFunc) {
        for (int i = 0; i < mappings.length; i++) {
            dst[i] = cloneFunc.apply(src[mappings[i]]);
        }
    }

    public Object @NonNull [] revMap(Object[] src) {
        Object[] dst = new Object[mappings.length];
        revMap(dst, src);
        return dst;
    }

    @Override
    public @NonNull String toString() {
        return Arrays.toString(mappings);
    }
}
