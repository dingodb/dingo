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

package io.dingodb.exec.aggregate;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("count")
public class CountAgg extends AbstractAgg {
    @Override
    public Object first(Object[] tuple) {
        return 1L;
    }

    @Override
    public Object add(Object var, Object[] tuple) {
        return (long) var + 1L;
    }

    @Override
    public Object merge(Object var1, Object var2) {
        if (var1 != null) {
            if (var2 != null) {
                return (long) var1 + (long) var2;
            }
            return var1;
        }
        return var2;
    }

    @Override
    public Object getValue(Object var) {
        return var != null ? var : 0L;
    }
}
