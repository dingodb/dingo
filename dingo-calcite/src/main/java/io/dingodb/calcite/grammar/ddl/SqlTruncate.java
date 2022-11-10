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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Base class for an TRUNCATE statements parse tree nodes. The portion of the
 * statement covered by this class is "TRUNCATE".
 */
public class SqlTruncate extends SqlDdl {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("TRUNCATE TABLE", SqlKind.OTHER_DDL);

    public SqlIdentifier id;

    /**
     * Creates a node.
     *
     * @param pos Parser position, must not be null.
    */
    public SqlTruncate(SqlParserPos pos) {
        super(OPERATOR, pos);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(id);
    }

    /**
     * Creates a node.
     *
     * @param pos Parser position, must not be null.
     */
    public SqlTruncate(SqlParserPos pos, SqlIdentifier id) {
        super(OPERATOR, pos);
        this.id = id;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print("truncate ");
        writer.print(id.toString());
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {

    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        return null;
    }

    @Override
    public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
        return false;
    }
}
