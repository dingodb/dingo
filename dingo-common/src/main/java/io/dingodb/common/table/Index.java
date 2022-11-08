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

package io.dingodb.common.table;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Index {

    @JsonProperty("name")
    private String name;

    @JsonProperty("columns")
    private String[] columns;

    @JsonProperty("unique")
    private boolean unique;

    @JsonProperty("status")
    private IndexStatus status;

    public Index() {
    }

    public Index(String indexName, String[] columnNames, boolean unique) {
        this.name = indexName;
        this.columns = columnNames;
        this.unique = unique;
        this.status = IndexStatus.NEW;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public IndexStatus getStatus() {
        return status;
    }

    public void setStatus(IndexStatus status) {
        this.status = status;
    }

    public void setBusy() {
        this.status = IndexStatus.BUSY;
    }

    public void setNormal() {
        this.status = IndexStatus.NORMAL;
    }

    public void setDeleted() {
        this.status = IndexStatus.DELETED;
    }

}
