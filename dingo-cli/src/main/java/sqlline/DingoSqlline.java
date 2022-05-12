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

package sqlline;

import io.dingodb.calcite.Connections;
import lombok.experimental.Delegate;

import java.sql.SQLException;

public class DingoSqlline {

    @Delegate
    private SqlLine sqlLine;

    @Delegate
    private DatabaseConnections databaseConnections;

    public DingoSqlline() {
        this.sqlLine = new SqlLine();
        SqlLineOpts opts = this.sqlLine.getOpts();
        opts.setTimestampFormat("yyyy-MM-dd HH:mm:ss");
        opts.set(BuiltInProperty.NULL_VALUE, "NULL");
        this.sqlLine.setOpts(opts);
        databaseConnections = sqlLine.getDatabaseConnections();
    }

    public void connect() throws SQLException {
        setConnection(new SqllineDatabaseConnection(this.sqlLine, Connections.getConnection()));
    }

}
