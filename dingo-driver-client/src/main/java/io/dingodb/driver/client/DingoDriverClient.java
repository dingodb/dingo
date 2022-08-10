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

import io.dingodb.common.Location;
import io.dingodb.driver.DingoServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.DingoRemoteMeta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.Service;

import java.util.Properties;
import java.util.function.Supplier;

@Slf4j
public class DingoDriverClient extends Driver {
    public static final String CONNECT_STRING_PREFIX = "jdbc:dingo:thin:";

    public static final String PARAM_SPLIT = "&";

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
        Properties info = getUrlParam(config.url());
        int timeout = 30;
        if (info.containsKey("timeout")) {
            timeout = Integer.parseInt(info.getProperty("timeout"));
        }
        log.info("driver info" + info);
        // Create a single Service and set it on the Connection instance
        Supplier<Location> locationSupplier = null;
        String[] split = info.getProperty("point").split(":");
        if (split.length == 2) {
            Location location = new Location(split[0], Integer.valueOf(split[1]));
            locationSupplier = () -> location;
        } else {
            throw new IllegalArgumentException("Bad url: " + config.url());
        }
        final Service service = new DingoServiceImpl(locationSupplier, timeout);
        connection.setService(service);
        return new DingoRemoteMeta(connection, service);
    }

    private Properties getUrlParam(String url) {
        Properties paramInfo = new Properties();
        int paramSplitIndex = 0;
        if ((paramSplitIndex = url.indexOf("?")) > 0) {
            String paramStr = url.substring(paramSplitIndex + 1);
            String[] params = paramStr.split(PARAM_SPLIT);
            for (String paramPair : params) {
                String[] paramKV = paramPair.split("=");
                if (paramKV.length == 2) {
                    paramInfo.put(paramKV[0], paramKV[1]);
                }
            }
            String point = url.substring(0, paramSplitIndex);
            paramInfo.put("point", point);
        } else {
            paramInfo.put("point", url);
        }
        return paramInfo;
    }
}
