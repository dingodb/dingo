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

package io.dingodb.driver.client;

import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.remote.Driver;

public class DingoDriverClient extends Driver {
    public static final String CONNECT_STRING_PREFIX = "jdbc:dingo:thin:";

    static final DriverVersion DRIVER_VERSION = new DriverVersion(
        "Dingo JDBC Thin Driver",
        "0.1.0",
        "DingoDB",
        "0.1.0",
        true,
        0,
        1,
        0,
        1
    );

    static {
        new DingoDriverClient().register();
    }

    @Override
    protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return DRIVER_VERSION;
    }
}
