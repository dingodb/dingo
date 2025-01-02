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


import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class DingoSqlCreateView extends SqlCreate {
    public final SqlIdentifier name;
    public final @Nullable SqlNodeList columnList;
    public final SqlNode query;
    private static final SqlOperator OPERATOR;
    public String security;
    public String alg;
    public String definer;
    public String host;
    public String checkOpt;

    public DingoSqlCreateView(
        SqlParserPos pos,
        boolean replace,
        SqlIdentifier name,
        @Nullable SqlNodeList columnList,
        SqlNode query,
        String security,
        String alg,
        String definer,
        String host,
        String checkOpt
    ) {
        super(OPERATOR, pos, replace, false);
        this.name = (SqlIdentifier) Objects.requireNonNull(name, "name");
        this.columnList = columnList;
        this.query = (SqlNode)Objects.requireNonNull(query, "query");
        this.security = security == null ? "" : security;
        this.alg = alg == null ? "" : security;
        this.definer = definer;
        this.host = host;
        this.checkOpt = checkOpt;
    }

    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(this.name, this.columnList, this.query);
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (this.getReplace()) {
            writer.keyword("CREATE OR REPLACE");
        } else {
            writer.keyword("CREATE");
        }
        if (alg != null) {
            writer.keyword("ALGORITHM =");
            writer.keyword(alg);
        }
        if (definer != null) {
            writer.keyword("DEFINER = ");
            writer.keyword(definer);
            if (host != null) {
                writer.keyword("@");
                writer.keyword(host);
            }
        }
        if (security != null) {
            writer.keyword("SQL SECURITY");
            writer.keyword(security);
        }


        writer.keyword("VIEW");
        this.name.unparse(writer, leftPrec, rightPrec);
        if (this.columnList != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            Iterator var5 = this.columnList.iterator();

            while(var5.hasNext()) {
                SqlNode c = (SqlNode)var5.next();
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }

            writer.endList(frame);
        }

        writer.keyword("AS");
        writer.newlineAndIndent();
        this.query.unparse(writer, 0, 0);
        if (checkOpt != null) {
            writer.keyword("WITH");
            writer.keyword(checkOpt);
            writer.keyword("CHECK OPTION");
        }
    }

    static {
        OPERATOR = new SqlSpecialOperator("CREATE VIEW", SqlKind.CREATE_VIEW);
    }
}
