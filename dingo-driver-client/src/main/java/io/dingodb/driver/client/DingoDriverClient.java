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

import io.dingodb.driver.DingoServiceImpl;
import io.dingodb.net.NetAddress;
import io.dingodb.net.NetAddressProvider;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.DingoRemoteMeta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.Service;

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

    @Override public Meta createMeta(AvaticaConnection connection) {
        final ConnectionConfig config = connection.config();

        // Create a single Service and set it on the Connection instance
        NetAddressProvider addressProvider = new NetAddressProvider() {
            @Override
            public NetAddress get() {
                NetAddress netAddress = null;
                String[] split = config.url().split(":");
                if (split.length == 2) {
                    netAddress = new NetAddress(split[0], Integer.valueOf(split[1]));
                }
                return netAddress;
            }
        };

        final Service service = new DingoServiceImpl(addressProvider);
        connection.setService(service);
        return new DingoRemoteMeta(connection, service);
    }

}
