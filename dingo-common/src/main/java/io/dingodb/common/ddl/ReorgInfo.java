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

package io.dingodb.common.ddl;

import io.dingodb.common.CommonId;
import io.dingodb.common.meta.SchemaInfo;
import lombok.Builder;
import lombok.Data;

@Data
public class ReorgInfo {
    private DdlJob ddlJob;
    private boolean first;
    private boolean mergingTmpIndex;
    private CommonId tableId;
    private CommonId indexId;
    private SchemaInfo schemaInfo;
    private MetaElement[] elements;
    private MetaElement element;

    @Builder
    public ReorgInfo(
        DdlJob ddlJob,
        boolean first,
        boolean mergingTmpIndex,
        CommonId tableId,
        SchemaInfo schemaInfo,
        MetaElement[] elements,
        MetaElement element,
        CommonId indexId
    ) {
        this.ddlJob = ddlJob;
        this.first = first;
        this.mergingTmpIndex = mergingTmpIndex;
        this.tableId = tableId;
        this.schemaInfo = schemaInfo;
        this.elements = elements;
        this.element = element;
        this.indexId = indexId;
    }

    public ReorgInfo() {
    }
}
