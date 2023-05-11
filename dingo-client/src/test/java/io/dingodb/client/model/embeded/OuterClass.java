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

package io.dingodb.client.model.embeded;


import io.dingodb.client.annotation.DingoEmbed;
import io.dingodb.client.annotation.DingoKey;
import io.dingodb.client.annotation.DingoRecord;

import java.util.ArrayList;
import java.util.List;

@DingoRecord(database = "test", table = "testSet")
public class OuterClass {
    @DingoKey
    public int id;
    @DingoEmbed(type = DingoEmbed.EmbedType.MAP, elementType = DingoEmbed.EmbedType.LIST)
    public List<OwnedClass> children;

    public OuterClass() {
        children = new ArrayList<>();
    }
}



