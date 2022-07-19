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

import io.dingodb.sdk.compute.number.ComputeNumber;
import io.dingodb.sdk.context.BasicContext;

import java.util.Arrays;

public class MinExecutive extends NumberExecutive<BasicContext, ComputeNumber, ComputeNumber> {

    @Override
    public ComputeNumber execute(BasicContext context, ComputeNumber... second) {
        Arrays.sort(second);
        // return ComputeNumber.min(first, second[second.length - 1]);
        return null;
    }
}
