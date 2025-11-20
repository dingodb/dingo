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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Getter;
import org.apache.calcite.schema.impl.ListTransientTable;

import java.util.Collection;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PROTECTED_AND_PUBLIC;

@Getter
@JsonTypeName("listTransientScanParam")
@JsonPropertyOrder({"listTransientTable"})
@JsonAutoDetect(fieldVisibility = PROTECTED_AND_PUBLIC)
public class ListTransientScanParam extends FilterProjectSourceParam {

    @JsonProperty("listTransientTable")
    Collection list;

    public ListTransientScanParam(ListTransientTable listTransientTable) {
        super(null, null, null, 0, null, null, null, 0);
        this.list = listTransientTable.getModifiableCollection();
    }
}
