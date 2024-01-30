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

package io.dingodb.meta.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.TupleMapping;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Getter
@ToString
@SuperBuilder
public class IndexTable extends Table {
    @JsonProperty
    public final IndexType indexType;
    @JsonProperty
    public final boolean unique;
    public final CommonId primaryId;
    public final TupleMapping mapping;
}
