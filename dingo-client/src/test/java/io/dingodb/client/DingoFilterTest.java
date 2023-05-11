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

package io.dingodb.client;

import io.dingodb.client.operation.filter.DingoFilter;
import io.dingodb.client.operation.filter.impl.DingoLogicalExpressFilter;
import io.dingodb.client.operation.filter.impl.DingoValueEqualsFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DingoFilterTest {

    @Test
    public void testFilter() {
        DingoFilter root = new DingoLogicalExpressFilter();
        DingoFilter equalsFilter = new DingoValueEqualsFilter(new int[]{3}, new Object[]{1});
        root.addAndFilter(equalsFilter);

        Object[] record = new Object[] {1, "a1", "a2", 1};
        Assertions.assertEquals(root.filter(record), true);

        record = new Object[] {1, "a1", "a2", 2};
        Assertions.assertEquals(root.filter(record), false);

        DingoFilter equalsFilter2 = new DingoValueEqualsFilter(new int[]{1}, new Object[]{"a1"});
        root.addAndFilter(equalsFilter2);

        record = new Object[] {1, "a1", "a2", 1};
        Assertions.assertEquals(root.filter(record), true);

        record = new Object[] {1, "a2", "a2", 1};
        Assertions.assertEquals(root.filter(record), false);

        root = new DingoLogicalExpressFilter();
        root.addOrFilter(equalsFilter);
        root.addOrFilter(equalsFilter2);

        Assertions.assertEquals(root.filter(record), true);

        record = new Object[] {1, "a2", "a2", 2};
        Assertions.assertEquals(root.filter(record), false);
    }
}
