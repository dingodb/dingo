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

package io.dingodb.example.model;

import io.dingodb.sdk.annotation.DingoColumn;
import io.dingodb.sdk.annotation.DingoKey;
import io.dingodb.sdk.annotation.DingoRecord;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

@DingoRecord(table = "modela")
@Getter
@Setter
@ToString
public class PojoModel {
    @DingoKey
    private int id;

    @DingoColumn(name = "name", scale = "20")
    private String name;

    private double salary;
    private boolean isMan;
    private Date birthday;

    @DingoColumn(name = "btime")
    private Time birthTime;

    @DingoColumn(name = "btimestamp")
    private Timestamp birthtimestamp;

    @DingoColumn(name = "address", elementType = "varchar")
    private List<String> address;


    @DingoColumn(name = "location", elementType = "varchar")
    private Set<String> location;


    @DingoColumn(name = "interest")
    private Map<Integer, String> interest;
}
