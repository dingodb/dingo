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

package io.dingodb.expr.json.schema;

import io.dingodb.expr.json.runtime.RtSchema;
import io.dingodb.expr.json.runtime.RtSchemaArray;
import io.dingodb.expr.json.runtime.RtSchemaRoot;
import io.dingodb.expr.runtime.TypeCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestSchemaParser {
    @Test
    public void testSimpleVars() throws Exception {
        RtSchemaRoot root = SchemaParser.YAML.parse(
            TestSchemaParser.class.getResourceAsStream("/simple_vars.yml")
        );
        assertThat(root.getMaxIndex()).isEqualTo(4);
        RtSchema schema = root.getSchema();
        assertThat(schema.getChild("a").getTypeCode()).isEqualTo(TypeCode.LONG);
        assertThat(schema.getChild("b").getTypeCode()).isEqualTo(TypeCode.DOUBLE);
        assertThat(schema.getChild("c").getTypeCode()).isEqualTo(TypeCode.BOOL);
        assertThat(schema.getChild("d").getTypeCode()).isEqualTo(TypeCode.STRING);
    }

    @Test
    public void testCompositeVars() throws Exception {
        RtSchemaRoot root = SchemaParser.YAML.parse(
            TestSchemaParser.class.getResourceAsStream("/composite_vars.yml")
        );
        assertThat(root.getMaxIndex()).isEqualTo(8);
        RtSchema schema = root.getSchema();
        assertThat(schema.getChild("arrA").getTypeCode()).isEqualTo(TypeCode.ARRAY);
        assertThat(((RtSchemaArray) schema.getChild("arrA")).getElementTypeCode()).isEqualTo(TypeCode.LONG);
        assertThat(schema.getChild("arrB").getTypeCode()).isEqualTo(TypeCode.ARRAY);
        assertThat(((RtSchemaArray) schema.getChild("arrB")).getElementTypeCode()).isEqualTo(TypeCode.STRING);
        assertThat(schema.getChild("arrC").getTypeCode()).isEqualTo(TypeCode.LIST);
        assertThat(schema.getChild("arrD").getTypeCode()).isEqualTo(TypeCode.TUPLE);
        assertThat(schema.getChild("arrD").getChild(0).getTypeCode()).isEqualTo(TypeCode.LONG);
        assertThat(schema.getChild("arrD").getChild(1).getTypeCode()).isEqualTo(TypeCode.STRING);
        assertThat(schema.getChild("mapA").getTypeCode()).isEqualTo(TypeCode.MAP);
        assertThat(schema.getChild("mapB").getTypeCode()).isEqualTo(TypeCode.DICT);
        assertThat(schema.getChild("mapB").getChild("foo").getTypeCode()).isEqualTo(TypeCode.DOUBLE);
        assertThat(schema.getChild("mapB").getChild("bar").getTypeCode()).isEqualTo(TypeCode.STRING);
    }
}
