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

package io.dingodb.store.row;

import com.alipay.sofa.jraft.option.CliOptions;
import io.dingodb.store.row.client.DefaultDingoRowStoreCliService;
import io.dingodb.store.row.client.DingoRowStoreCliService;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class DingoRowStoreServiceFactory {
    /**
     * Create and initialize a DingoRowStoreCliService instance.
     */
    public static DingoRowStoreCliService createAndInitDingoRowStoreCliService(final CliOptions opts) {
        final DingoRowStoreCliService cliService = new DefaultDingoRowStoreCliService();
        if (!cliService.init(opts)) {
            throw new IllegalStateException("Fail to init DingoRowStoreCliService");
        }
        return cliService;
    }

    private DingoRowStoreServiceFactory() {
    }
}
