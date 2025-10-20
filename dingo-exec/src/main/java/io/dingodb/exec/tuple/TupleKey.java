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

package io.dingodb.exec.tuple;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;

/**
 * Wrap tuples to provide hash and equals.
 */
@RequiredArgsConstructor
public class TupleKey {
    @Getter
    private final Object[] tuple;

    @Override
    public int hashCode() {
        return Arrays.hashCode(tuple);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TupleKey) {
            return equals(this.tuple, ((TupleKey) obj).tuple);
        }
        return false;
    }

    public boolean equals(Object[] tuple, Object[] tuple2) {
        if (tuple == tuple2) {
            return true;
        }
        if (tuple == null || tuple2 == null) {
            return false;
        }
        int length = tuple.length;
        if (tuple2.length != length) {
            return false;
        }

        for (int i = 0; i < length; i++) {
            Object o1 = tuple[i];
            Object o2 = tuple2[i];
            if (o1 instanceof Integer && o2 instanceof Long) {
                Long longVal = (Long) o2;
                Integer intVal = (Integer) o1;
                if (longVal.intValue() != intVal) {
                    return false;
                }
            } else if (o1 instanceof Long && o2 instanceof Integer) {
                Integer intVal = (Integer) o2;
                Long longVal = (Long) o1;
                if (longVal.intValue() != intVal) {
                    return false;
                }
            } else if (!(o1 == null ? o2 == null : o1.equals(o2))) {
                return false;
            }
        }

        return true;
    }

}
