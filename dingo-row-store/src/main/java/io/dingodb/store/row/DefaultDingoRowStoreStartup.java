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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.dingodb.store.row.client.DefaultDingoRowStore;
import io.dingodb.store.row.errors.StoreStartupException;
import io.dingodb.store.row.options.DingoRowStoreOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class DefaultDingoRowStoreStartup {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDingoRowStoreStartup.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            LOG.error("Usage: io.dingodb.store.row.DefaultDingoRowStoreStartup <ConfigFilePath>");
            System.exit(1);
        }
        final String configPath = args[0];
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final DingoRowStoreOptions opts = mapper.readValue(new File(configPath),
            DingoRowStoreOptions.class);
        final DefaultDingoRowStore store = new DefaultDingoRowStore();
        if (!store.init(opts)) {
            throw new StoreStartupException("Fail to start [DefaultDingoRowStoreStartup].");
        }
        LOG.info("Starting DefaultDingoRowStore with config: {}.", opts);
    }
}
