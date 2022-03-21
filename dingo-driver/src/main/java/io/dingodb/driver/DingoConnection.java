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

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.DingoRootSchema;
import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.RelRunner;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import javax.annotation.Nonnull;

public class DingoConnection extends AvaticaConnection {
    @Getter
    private final DingoParserContext context;

    protected DingoConnection(
        DingoDriver driver,
        AvaticaFactory factory,
        String url,
        Properties info
    ) {
        super(driver, factory, url, info);
        String defaultSchema = info.getProperty("defaultSchema");
        if (defaultSchema == null) {
            defaultSchema = DingoRootSchema.DEFAULT_SCHEMA_NAME;
        }
        context = new DingoParserContext(defaultSchema);
    }

    public DingoStatement getStatement(@Nonnull Meta.StatementHandle sh) throws SQLException {
        // Returns a new statement if not exists.
        return (DingoStatement) lookupStatement(sh);
    }

    public AvaticaResultSet newResultSet(
        AvaticaStatement statement,
        Meta.Signature sig,
        Meta.Frame firstFrame,
        String sql
    ) throws SQLException {
        final TimeZone timeZone = getTimeZone();
        return factory.newResultSet(statement, new QueryState(sql), sig, timeZone, firstFrame);
    }

    public DingoContext createContext() {
        return new DingoContext(this);
    }

    static class DingoContext implements CalcitePrepare.Context {
        private final DingoConnection connection;

        DingoContext(DingoConnection connection) {
            this.connection = connection;
        }

        public DingoParserContext getParserContext() {
            return connection.context;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return connection.context.getTypeFactory();
        }

        @Override
        public CalciteSchema getRootSchema() {
            return connection.context.getRootSchema();
        }

        @Override
        public CalciteSchema getMutableRootSchema() {
            return connection.context.getRootSchema();
        }

        @Override
        public List<String> getDefaultSchemaPath() {
            return ImmutableList.of(connection.context.getDefaultSchemaName());
        }

        @Override
        public CalciteConnectionConfig config() {
            return null;
        }

        @Override
        public CalcitePrepare.SparkHandler spark() {
            return null;
        }

        @Override
        public DataContext getDataContext() {
            return new DingoDataContext(connection);
        }

        @Override
        public @Nullable List<String> getObjectPath() {
            return null;
        }

        @Override
        public RelRunner getRelRunner() {
            return null;
        }
    }

    static class DingoDataContext implements DataContext {
        private final DingoConnection connection;

        DingoDataContext(DingoConnection connection) {
            this.connection = connection;
        }

        @Override
        public @Nullable SchemaPlus getRootSchema() {
            CalciteSchema rootSchema = connection.context.getRootSchema();
            return rootSchema == null ? null : rootSchema.plus();
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return connection.context.getTypeFactory();
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public @Nullable Object get(String name) {
            return null;
        }
    }
}
