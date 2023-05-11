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

package io.dingodb.client.udf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.JsePlatform;

public class LuaTest {

    @Test
    public void testLuaFunction() {
        LuaTable table = new LuaTable();
        table.set(0, LuaValue.valueOf(true));
        table.set(1, LuaValue.valueOf(false));
        table.set(2, LuaValue.valueOf(false));

        Assertions.assertEquals(table.len().toint(), table.length());
        Assertions.assertTrue(table.get(0).toboolean());
        Assertions.assertTrue(!table.get(1).toboolean());
        Assertions.assertTrue(!table.get(2).toboolean());

        LuaTable table1 = new LuaTable();
        table1.set(1, table);
        Globals globals = JsePlatform.standardGlobals();

        String luajFunction = "function test(o) \r\n"
            + "    o[1][1] = true \r\n"
            + "    return o \r\n"
            + "end";

        globals.load(luajFunction).call();

        LuaValue udf = globals.get(LuaValue.valueOf("test"));
        LuaValue result = udf.call(table1);
        System.out.println(result.get(1).get(0));
        System.out.println(result.get(1).get(1));
        System.out.println(result.get(1).get(2));
    }
}
