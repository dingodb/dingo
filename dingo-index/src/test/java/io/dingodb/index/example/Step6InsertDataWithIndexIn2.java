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

package io.dingodb.index.example;

public class Step6InsertDataWithIndexIn2 extends Step0Info {
    public static void main(String[] args) throws Exception {
        dingoIndexDataExecutor.executeInsert(tableName, getData3().get(0));
        dingoIndexDataExecutor.executeInsert(tableName, getData3().get(1));
        dingoIndexDataExecutor.executeInsert(tableName, getData3().get(2));
        dingoIndexDataExecutor.executeInsert(tableName, getData3().get(3));
        dingoIndexDataExecutor.executeInsert(tableName, getData3().get(4));
    }
}
