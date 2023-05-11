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

package io.dingodb.client.model;

import io.dingodb.client.annotation.DingoEmbed;
import io.dingodb.client.annotation.DingoKey;
import io.dingodb.client.annotation.DingoRecord;
import lombok.Getter;
import lombok.Setter;

@DingoRecord(database = "test", table = "testSet")
@Setter
@Getter
public class AnnotatedArrayClass {
    @DingoKey
    private int key;
    private byte[] bytes;
    private short[] shorts;
    private int[] ints;
    private long[] longs;
    private float[] floats;
    private double[] doubles;
    private String[] strings;
    @DingoEmbed
    private ChildClass[] children;

    @DingoEmbed(elementType = DingoEmbed.EmbedType.LIST)
    private ChildClass[] listChildren;

    @DingoEmbed(elementType = DingoEmbed.EmbedType.MAP)
    private ChildClass[] mapChildren;
}
