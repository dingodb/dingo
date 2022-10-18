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

package io.dingodb.common.partition;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

@Getter
@Setter
@ToString
public class DingoTablePart implements Serializable {

    private static final long serialVersionUID = 2252446672472101114L;

    @JsonProperty("funcNm")
    String funcNm;

    @JsonProperty("cols")
    List<String> cols;

    @JsonProperty("partSize")
    Integer partSize;

    @JsonProperty("partDetailList")
    List<DingoPartDetail> partDetailList;

    public DingoTablePart(String funcNm, List<String> cols) {
        this.funcNm = funcNm;
        this.cols = cols;
    }

    public DingoTablePart() {
    }

    /**
     * 获取分区键的index.
     * @param definition tableDefinition
     * @return TupleMapping
     */
    public TupleMapping getPartMapping(TableDefinition definition) {
        List<String> cols = definition.getDingoTablePart().getCols();
        List<Integer> indices = new LinkedList<>();
        List<ColumnDefinition> columnList = definition.getColumns();
        for (String columnNm : cols) {
            for (int i = 0; i < columnList.size(); i ++) {
                if (columnNm.equalsIgnoreCase(columnList.get(i).getName())) {
                    indices.add(i);
                    break;
                }
            }
        }
        return TupleMapping.of(indices);
    }
}
