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

package io.dingodb.client.model.basic;

import io.dingodb.client.annotation.DingoColumn;
import io.dingodb.client.annotation.DingoRecord;
import lombok.Getter;
import lombok.Setter;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

@Getter
@Setter
@DingoRecord(database = "test", table = "people")
public class PersonWithOutKey {
    private String ssn;

    @DingoColumn(name  =  "frstNme")
    private String firstName;

    @DingoColumn(name = "lstNme")
    private String lastName;

    @DingoColumn(name = "age")
    private int age;

    @DingoColumn(name = "salary")
    private double salary;

    @DingoColumn(name = "salary1")
    private float  salary2;

    @DingoColumn(name = "birthday")
    private Date birthday;

    @DingoColumn(name = "birthTime")
    private Time birthTime;

    @DingoColumn(name = "birthTimeStamp")
    private Timestamp birthTimestamp;
}
