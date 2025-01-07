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

package io.dingodb.calcite.grammar.ddl;

import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

public class ColumnOption {
    public boolean nullable = true;
    public boolean autoIncrement;
    public boolean primaryKey;
    public boolean clustered = true;
    public boolean global = true;

    public boolean uniqueKey;
    public SqlNode expression;
    public boolean serialDefaultVal;
    public String comment;
    public Constraint constraint;
    public SqlNodeList refColumnList;
    public SqlIdentifier refTable;
    public String updateRefOpt;
    public String deleteRefOpt;
    public String collate = "utf8_bin";
    public String columnFormat;
    public String storage;
    public int autoRandom;
    public boolean visible;
    public ColumnStrategy strategy;

    public ColumnOption() {
    }
}
