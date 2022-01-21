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

package io.dingodb.test.driver.server;

import io.dingodb.driver.server.ServerMetaFactory;
import io.dingodb.exec.Services;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;

public final class DingoDriverTestServer {
    private DingoDriverTestServer() {
    }

    public static void main(String[] args) throws Exception {
        Services.META.init(null);
        Services.initNetService();
        HttpServer server = Main.start(
            new String[]{ServerMetaFactory.class.getCanonicalName()},
            8765,
            AvaticaJsonHandler::new
        );
        server.join();
    }
}
