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

package io.dingodb.exec.transaction.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import static io.protostuff.CollectionSchema.MessageFactories.List;

public class TransactionCache {

    public byte[] getPrimaryKey() {
        // call StoreService
        return new byte[]{};
    }

    public Iterator<Object[]> iterator() {
        return new ArrayList<Object[]>(Collections.singleton(new Object[]{})).iterator();
    }
}
