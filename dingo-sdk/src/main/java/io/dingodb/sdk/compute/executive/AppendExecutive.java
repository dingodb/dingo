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

package io.dingodb.sdk.compute.executive;

import io.dingodb.sdk.compute.str.ComputeString;
import io.dingodb.sdk.context.BasicContext;

import java.util.Arrays;

public class AppendExecutive extends KVExecutive<BasicContext, ComputeString, ComputeString> {

    @Override
    public ComputeString execute(BasicContext context, ComputeString... second) {
        ComputeString record = ComputeString.of(context.record[0].toString());
        return record.append(Arrays.stream(second).reduce((a, b) -> a.append(b)).get());
    }
}
