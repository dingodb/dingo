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

package io.dingodb.driver.plancache;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import lombok.Builder;
import org.apache.calcite.avatica.ColumnMetaData;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.Builder;


@Setter
@Getter
@Builder
@ToString
public class PlanCacheKey {
    String queryStr;
    boolean stmtCacheable;
    Object preparedAst;
    String dbName;
    List<String> tbls;
    Map<Object, Object> relateVersion;
    int schemaVersion;
    Object pointGet;
    List<String> outputColumns;
    String planDigest;
    int limits;
    boolean hasSubquery;
    List<String> hints;

}
