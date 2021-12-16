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

package io.dingodb.dingokv;

import com.alipay.sofa.jraft.option.CliOptions;
import io.dingodb.dingokv.client.DefaultDingoKVCliService;
import io.dingodb.dingokv.client.DingoKVCliService;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class DingoKVServiceFactory {
    /**
     * Create and initialize a DingoKVCliService instance.
     */
    public static DingoKVCliService createAndInitDingoKVCliService(final CliOptions opts) {
        final DingoKVCliService cliService = new DefaultDingoKVCliService();
        if (!cliService.init(opts)) {
            throw new IllegalStateException("Fail to init DingoKVCliService");
        }
        return cliService;
    }

    private DingoKVServiceFactory() {
    }
}
