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

 package io.dingodb.calcite.grammar.dml;

 import com.google.common.collect.ImmutableList;
 import lombok.Getter;
 import org.apache.calcite.sql.SqlIdentifier;
 import org.apache.calcite.sql.SqlNode;
 import org.apache.calcite.sql.SqlNodeList;
 import org.apache.calcite.sql.SqlNumericLiteral;
 import org.apache.calcite.sql.SqlSelect;
 import org.apache.calcite.sql.parser.SqlParserPos;
 import org.checkerframework.checker.nullness.qual.Nullable;

 import java.util.Collections;

 @Getter
 public class SqlUpdate extends org.apache.calcite.sql.SqlUpdate {

     @Getter
     public long limit = -1L;
     public SqlNodeList tableList;
     public SqlNodeList aliasList;

     public SqlUpdate(SqlParserPos pos,
                      SqlNode targetTable,
                      SqlNodeList targetColumnList,
                      SqlNodeList sourceExpressionList,
                      @Nullable SqlNode condition,
                      @Nullable SqlSelect sourceSelect,
                      @Nullable SqlIdentifier alias,
                      SqlNode offsetFetch) {
         this(pos,
             targetTable,
             targetColumnList,
             sourceExpressionList,
             condition,
             sourceSelect,
             alias,
             offsetFetch,
             new SqlNodeList(ImmutableList.of(targetTable), SqlParserPos.ZERO),
             new SqlNodeList(ImmutableList.of(alias), SqlParserPos.ZERO));
     }

     public SqlUpdate(SqlParserPos pos,
                      SqlNode targetTable,
                      SqlNodeList targetColumnList,
                      SqlNodeList sourceExpressionList,
                      @Nullable SqlNode condition,
                      @Nullable SqlSelect sourceSelect,
                      @Nullable SqlIdentifier alias,
                      SqlNode offsetFetch,
                      SqlNodeList tableList,
                      SqlNodeList aliasList) {
         super(pos,
             targetTable,
             tableList,
             aliasList,
             Collections.emptyMap(),
             targetColumnList,
             sourceExpressionList,
             condition,
             sourceSelect,
             alias,
             true);
         if (offsetFetch != null && offsetFetch instanceof SqlNumericLiteral) {
             limit = ((SqlNumericLiteral) offsetFetch).longValue(true);
         }
         this.tableList = tableList;
         this.aliasList = aliasList;
     }

 }
