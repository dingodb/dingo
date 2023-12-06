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

import com.google.auto.service.AutoService;
import io.dingodb.verify.service.InformationService;
import io.dingodb.verify.service.InformationServiceProvider;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(InformationServiceProvider.class)
public class InformationTestServiceProvider implements io.dingodb.verify.service.InformationServiceProvider {

    @Override
    public InformationService get() {
        return InformationTestService.INSTANCE;
    }
}
