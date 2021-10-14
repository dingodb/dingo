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

package io.dingodb.expr.json.runtime;

import io.dingodb.expr.runtime.CompileContext;

import java.io.Serializable;

public abstract class RtSchema implements CompileContext, Serializable {
    private static final long serialVersionUID = -8236170594700377496L;

    /**
     * Create index of this RtSchema.
     *
     * @param start the start number of the index
     * @return the max index number in this RtSchema plus 1
     */
    public abstract int createIndex(int start);

    /**
     * Get the index of this RtSchema if it is a leaf schema, else -1.
     *
     * @return the index or -1
     */
    public abstract int getIndex();
}
