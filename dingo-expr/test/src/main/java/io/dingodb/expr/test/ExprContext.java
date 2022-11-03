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

package io.dingodb.expr.test;

import io.dingodb.expr.json.runtime.DataFormat;
import io.dingodb.expr.json.runtime.DataParser;
import io.dingodb.expr.json.runtime.RtSchema;
import io.dingodb.expr.json.runtime.RtSchemaRoot;
import io.dingodb.expr.json.schema.SchemaParser;
import io.dingodb.expr.runtime.TupleEvalContext;
import lombok.Getter;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ExprContext implements BeforeAllCallback {
    private final String ctxFileName;
    private final String[] etxStrings;

    @Getter
    private RtSchemaRoot schemaRoot;
    private Object[][] tuples;

    /**
     * Create a ContextResource.
     *
     * @param ctxFileName the file name of the schema
     * @param etxStrings  several datum in YAML format
     */
    public ExprContext(String ctxFileName, String... etxStrings) {
        this.ctxFileName = ctxFileName;
        this.etxStrings = etxStrings;
    }

    /**
     * Get the RtSchema corresponding the schema file.
     *
     * @return the RtSchema
     */
    public RtSchema getCtx() {
        return schemaRoot.getSchema();
    }

    /**
     * Get the RtData corresponding to the data.
     *
     * @param index the index of the data
     * @return the RtData
     */
    public TupleEvalContext getEtx(int index) {
        return new TupleEvalContext(tuples[index]);
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        schemaRoot = SchemaParser.get(DataFormat.fromExtension(ctxFileName))
            .parse(ExprContext.class.getResourceAsStream(ctxFileName));
        DataParser parser = DataParser.yaml(schemaRoot);
        tuples = new Object[etxStrings.length][];
        for (int i = 0; i < tuples.length; i++) {
            tuples[i] = parser.parse(etxStrings[i]);
        }
    }
}
