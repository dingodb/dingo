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

package io.dingodb.calcite;

import io.dingodb.calcite.rule.DingoRules;
import io.dingodb.ddl.DingoDdlParserFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.runtime.Hook;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;
import javax.annotation.Nonnull;

public final class Connections {
    static {
    }

    private Connections() {
    }

    private static void registerRules(@Nonnull RelOptPlanner planner) {
        DingoRules.rules().forEach(planner::addRule);
    }

    public static Connection getConnection() throws SQLException {
        return getConnection(DingoRootSchema.DEFAULT_SCHEMA_NAME);
    }

    public static Connection getConnection(String defaultSchema) throws SQLException {
        try {
            Class.forName("io.dingodb.driver.DingoDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        Properties properties = new Properties();
        properties.setProperty("defaultSchema", defaultSchema);
        TimeZone timeZone = TimeZone.getDefault();
        properties.setProperty("timeZone", timeZone.getID());
        return DriverManager.getConnection("jdbc:dingo:", properties);
    }

    @SuppressWarnings("unused")
    public static Connection getCalciteConnection(String defaultSchema) throws SQLException {
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        Hook.PLANNER.addThread(o -> {
            registerRules((RelOptPlanner) o);
        });
        Properties props = new Properties();
        String sb = "inline: {\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: '" + defaultSchema + "',\n"
            + "  schemas: [ {\n"
            + "    type: 'custom',\n"
            + "    name: '" + defaultSchema + "',\n"
            + "    factory: '" + DingoSchemaFactory.class.getCanonicalName() + "',\n"
            + "    operand: {\n"
            + "    }\n"
            + "  } ]\n"
            + "}";
        props.put("model", sb);
        props.put("parserFactory", DingoDdlParserFactory.class.getCanonicalName() + "#INSTANCE");
        return DriverManager.getConnection("jdbc:calcite:", props);
    }
}
