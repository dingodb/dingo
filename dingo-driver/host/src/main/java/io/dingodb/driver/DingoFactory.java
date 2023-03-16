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

package io.dingodb.driver;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaSpecificDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

@Slf4j
public class DingoFactory implements AvaticaFactory {
    public static final DingoFactory INSTANCE = new DingoFactory();

    @Override
    public int getJdbcMajorVersion() {
        return 4;
    }

    @Override
    public int getJdbcMinorVersion() {
        return 1;
    }

    @Override
    public DingoConnection newConnection(
        UnregisteredDriver driver,
        AvaticaFactory factory,
        String url,
        Properties info
    ) {
        return new DingoConnection(
            (DingoDriver) driver,
            factory,
            url,
            info
        );
    }

    @Override
    public DingoStatement newStatement(
        AvaticaConnection connection,
        Meta.StatementHandle handle,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) {
        if (log.isDebugEnabled()) {
            log.debug("connection handle = {}, statement handle = {}.", connection.handle, handle);
        }
        return new DingoStatement(
            (DingoConnection) connection,
            handle,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability
        );
    }

    @Override
    public DingoPreparedStatement newPreparedStatement(
        AvaticaConnection connection,
        Meta.StatementHandle handle,
        Meta.Signature signature,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("connection handle = {}, statement handle = {}.", connection.handle, handle);
        }
        return new DingoPreparedStatement(
            (DingoConnection) connection,
            handle,
            signature,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability
        );
    }

    @Override
    public AvaticaResultSet newResultSet(
        AvaticaStatement statement,
        QueryState state,
        Meta.Signature signature,
        TimeZone timeZone,
        Meta.Frame firstFrame
    ) throws SQLException {
        final ResultSetMetaData metaData = newResultSetMetaData(statement, signature);
        if (signature instanceof DingoSignature || signature instanceof DingoExplainSignature
            || signature instanceof MysqlSignature) {
            return new DingoResultSet(statement, state, signature, metaData, timeZone, firstFrame);
        }
        return new AvaticaResultSet(statement, state, signature, metaData, timeZone, firstFrame);
    }

    @Override
    public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
        return new DingoDatabaseMetaData(connection);
    }

    @Override
    public ResultSetMetaData newResultSetMetaData(
        AvaticaStatement statement,
        Meta.Signature signature
    ) {
        return new AvaticaResultSetMetaData(statement, null, signature);
    }

    // Must inherit, the constructor of the base class is protected.
    private static class DingoDatabaseMetaData extends AvaticaDatabaseMetaData {
        DingoDatabaseMetaData(AvaticaConnection connection) {
            super(connection);
        }
    }
}
