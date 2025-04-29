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

package io.dingodb.calcite.utils;

import com.google.common.collect.Iterables;
import io.dingodb.calcite.schema.SubCalciteSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Base64;
import java.util.List;

public class DingoSqlValidatorUtil {

    public static void main(String[] args) {
        String str = "ZHJvcFRhYmxlRXJyb3I6V3JpdGVDb25mbGljdChzdGFydFRzPTQ0MDY3NjY5MzkwOTE3NzQ0LCBwcmltYXJ5S2V5PW51bGwsIGNvbmZsaWN0VHM9NDQwNjc2NjkzOTA5MTc3NDUsIGtleT1bMTE2LCAwLCAwLCAwLCAwLCAwLCAwLCAwLCAwLCAxMDksIDg0LCA5NywgOTgsIDEwOCwgMTAxLCA1OCwgNTQsIDUyLCAtMSwgNTAsIDU3LCA1MCwgMCwgMCwgMCwgMCwgMCwgLTYsIDAsIDAsIDAsIDAsIDAsIDAsIDAsIDEwNCwgNzMsIDExMCwgMTAwLCAxMDEsIDEyMCwgNTgsIDU0LCA1MiwgLTEsIDUzLCA0OCwgNTMsIDAsIDAsIDAsIDAsIDAsIC02XSwgY29uZmxpY3RDb21taXRUcz0wLCByZWFzb249T3B0aW1pc3RpYywgZXh0JD1udWxsKQ==";
        System.out.println(new String(Base64.getDecoder().decode(str)));
    }

    public static CalciteSchema.@Nullable TableEntry getTableEntry(
        SqlValidatorCatalogReader catalogReader, List<String> names) {
        // First look in the default schema, if any.
        // If not found, look in the root schema.
        for (List<String> schemaPath : catalogReader.getSchemaPaths()) {
            CalciteSchema schema =
                SqlValidatorUtil.getSchema(catalogReader.getRootSchema(),
                    Iterables.concat(schemaPath, Util.skipLast(names)),
                    catalogReader.nameMatcher());
            if (schema == null) {
                continue;
            }
            CalciteSchema.TableEntry entry =
                getTableEntryFrom(schema, Util.last(names),
                    catalogReader.nameMatcher().isCaseSensitive());
            if (entry != null) {
                return entry;
            }
        }
        return null;
    }

    private static CalciteSchema.@Nullable TableEntry getTableEntryFrom(
        CalciteSchema schema, String name, boolean caseSensitive) {
        CalciteSchema.TableEntry entry;
        if (schema instanceof SubCalciteSchema) {
            SubCalciteSchema subCalciteSchema = (SubCalciteSchema) schema;
            entry =
                subCalciteSchema.getImplicitTable(name, caseSensitive);
        } else {
            entry =
                schema.getTable(name, caseSensitive);
        }
        if (entry == null) {
            entry = schema.getTableBasedOnNullaryFunction(name, caseSensitive);
        }
        return entry;
    }

}
