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

package io.dingodb.test;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.MethodOrdererContext;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomTestOrder implements MethodOrderer {
    private static final List<String> methodSequence = ImmutableList.of(
        "testExplainSimpleValues",
        "testExplainInsertValues",
        "testExplainScan",
        "testSimpleValues",
        "testInsert",
        "testScan",
        "testScan2",
        "testGetByKey",
        "testGetByKey1",
        "testGetByKey2",
        "testFilterScan",
        "testProjectScan",
        "testCount",
        "testCount1",
        "testCount2",
        "testSum",
        "testSum1",
        "testUpdate",
        "testUpdate1",
        "testDelete",
        "testDelete1"
    );

    @Override
    public void orderMethods(MethodOrdererContext context) {
        final Map<String, Integer> sequenceMap = new HashMap<>(methodSequence.size());
        int count = 0;
        for (String s : methodSequence) {
            sequenceMap.put(s, count);
            ++count;
        }
        context.getMethodDescriptors().sort(Comparator.comparing(
            m -> sequenceMap.get(m.getMethod().getName())
        ));
    }
}
