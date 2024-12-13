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

import io.dingodb.common.sequence.SequenceDefinition;
import io.dingodb.meta.SequenceService;

public class SequenceTestService implements SequenceService {

    @Override
    public boolean existsSequence(String name) {
        return false;
    }

    @Override
    public void createSequence(SequenceDefinition definition) {

    }

    @Override
    public void dropSequence(String name) {

    }

    @Override
    public SequenceDefinition getSequence(String name) {
        return null;
    }

    @Override
    public Long nextVal(String name) {
        return null;
    }

    @Override
    public Long setVal(String name, Long seq) {
        return null;
    }

    @Override
    public Long currVal(String name) {
        return null;
    }

    @Override
    public Long lastVal(String name) {
        return null;
    }
}
