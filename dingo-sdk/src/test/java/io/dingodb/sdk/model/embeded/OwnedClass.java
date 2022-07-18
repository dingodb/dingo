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

package io.dingodb.sdk.model.embeded;

import io.dingodb.sdk.annotation.DingoKey;
import io.dingodb.sdk.annotation.DingoRecord;
import io.dingodb.sdk.annotation.ParamFrom;

import java.util.Date;

@DingoRecord(database = "test", table = "testSet")
public class OwnedClass {
    public String name;
    @DingoKey
    public int id;
    public Date date;

    public OwnedClass(@ParamFrom("name") String name, @ParamFrom("id") int id, @ParamFrom("date") Date date) {
        this.name = name;
        this.id = id;
        this.date = date;
    }
}
