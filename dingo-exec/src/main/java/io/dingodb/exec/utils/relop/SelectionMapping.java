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

package io.dingodb.exec.utils.relop;

import java.util.List;
import java.util.OptionalInt;
import java.util.stream.IntStream;

public class SelectionMapping {

    public static int findIndex(List<Integer> selection, Integer index) {
        OptionalInt newIndex = IntStream.range(0, selection.size())
            .filter(i -> selection.get(i).equals(index))
            .findFirst();
        if (newIndex.isPresent()) {
            return newIndex.getAsInt();
        } else {
            throw new IllegalArgumentException("Can't find index " + index + " in selection " + selection);
        }
    }
}
