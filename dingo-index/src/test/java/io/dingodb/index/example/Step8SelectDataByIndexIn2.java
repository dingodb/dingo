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

import java.util.List;

public class Step8SelectDataByIndexIn2 extends Step0Info {
    public static void main(String[] args) throws Exception {
        List<Object[]> r1 = dingoIndexDataExecutor.getRecordsByIndex(tableName, getIndexIn2().getName(), getSelectByIn2Data().get(0));
        List<Object[]> r2 = dingoIndexDataExecutor.getRecordsByIndex(tableName, getIndexIn2().getName(), getSelectByIn2Data().get(1));
        List<Object[]> r3 = dingoIndexDataExecutor.getRecordsByIndex(tableName, getIndexIn2().getName(), getSelectByIn2Data().get(2));

        System.out.println("r1 = " + r1);
        System.out.println("r2 = " + r2);
        System.out.println("r3 = " + r3);
    }
}
