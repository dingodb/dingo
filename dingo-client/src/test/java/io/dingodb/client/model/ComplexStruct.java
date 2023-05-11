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

import io.dingodb.client.annotation.DingoColumn;
import io.dingodb.client.annotation.DingoKey;
import io.dingodb.client.annotation.DingoRecord;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@DingoRecord(database = "test", table = "complexStruct")
public class ComplexStruct {
    @DingoKey
    private String ssn;

    @DingoColumn(name = "values")
    private List<Integer> values;

    @DingoColumn(name = "address")
    private List<Address> addresses;

    @DingoColumn(name = "home")
    private Map<String, String> home;

    @DingoColumn(name = "work")
    private Map<String, Address> work;

    @DingoColumn
    private Integer[] integerValues;
}
