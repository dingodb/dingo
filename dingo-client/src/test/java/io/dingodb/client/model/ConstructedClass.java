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

import io.dingodb.client.annotation.DingoKey;
import io.dingodb.client.annotation.DingoRecord;
import io.dingodb.client.annotation.ParamFrom;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
@DingoRecord(database = "test", table = "testSet")
public class ConstructedClass {
    @DingoKey
    public final int id;
    public final int age;
    public final String name;
    public Date birthday;

    public ConstructedClass(@ParamFrom("id") int id,
                            @ParamFrom("age") int age,
                            @ParamFrom("name") String name,
                            @ParamFrom("birthday") Date birthday) {
        super();
        this.id  =  id;
        this.age  =  age;
        this.name  =  name;
        this.birthday  =  birthday;
    }
}
