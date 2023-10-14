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
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.util.NoBreakFunctions;
import io.dingodb.driver.DingoClientFactory;
import io.dingodb.driver.DingoServiceImpl;
import io.dingodb.driver.api.MetaApi;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.net.netty.NetConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Helper;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.DingoRemoteMeta;
import org.apache.calcite.avatica.remote.Driver;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.function.Supplier;

@Slf4j
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

    private Properties props;

    protected static String[] parseHostPortPair(String hostPortPair) throws SQLException {
        String[] splitValues = new String[2];
        int portIndex = hostPortPair.indexOf(":");
        if (portIndex != -1) {
            if (portIndex + 1 >= hostPortPair.length()) {
                throw new SQLException("Malformed database URL, failed to parse the main URL sections.", "01S00", null);
            }

            String portAsString = hostPortPair.substring(portIndex + 1);
            String hostname = hostPortPair.substring(0, portIndex);
            splitValues[0] = hostname;
            splitValues[1] = portAsString;
        } else {
            splitValues[0] = hostPortPair;
            splitValues[1] = null;
        }
        return splitValues;
    }

    public static boolean startsWithIgnoreCase(String searchIn, String searchFor) {
        return startsWithIgnoreCase(searchIn, 0, searchFor);
    }

    public static boolean startsWithIgnoreCase(String searchIn, int startAt, String searchFor) {
        return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
    }

    @Override
    protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return DRIVER_VERSION;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //env.setRole(DingoRole.JDBC);
        if ((props = this.parseURL(url, info)) == null) {
            return null;
        } else {
            log.info("info:" + props);
            env.putAll(props);
            try {
                return super.connect(url, info);
            } catch (Throwable e) {
                throw Helper.INSTANCE.createException("connect failed");
            }
        }
    }

    @Override
    protected AvaticaFactory createFactory() {
        return new DingoClientFactory(instantiateFactory(getFactoryClassName(JdbcVersion.current())));
    }

    private Properties parseURL(String url, Properties defaults) throws SQLException {
        Properties urlProps = defaults;
        if (url == null) {
            return null;
        } else if (!startsWithIgnoreCase(url, CONNECT_STRING_PREFIX)) {
            return null;
        } else {
            try {
                InetAddress localHost = InetAddress.getLocalHost();
                urlProps.put("host", localHost.getHostAddress());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            int beginningOfSlashes = url.indexOf("url=");
            int index = url.indexOf("?");
            String hostStuff;
            String configNames;
            String configVal;
            if (index != -1) {
                hostStuff = url.substring(index + 1);
                url = url.substring(0, index);
                StringTokenizer queryParams = new StringTokenizer(hostStuff, "&");
                while (queryParams.hasMoreTokens()) {
                    String parameterValuePair = queryParams.nextToken();
                    int indexOfEquals = parameterValuePair.indexOf("=");
                    configNames = null;
                    configVal = null;
                    if (indexOfEquals != -1) {
                        configNames = parameterValuePair.substring(0, indexOfEquals);
                        if (indexOfEquals + 1 < parameterValuePair.length()) {
                            configVal = parameterValuePair.substring(indexOfEquals + 1);
                        }
                    }

                    if (configVal != null && configVal.length() > 0 && configNames != null
                        && configNames.length() > 0) {
                        try {
                            urlProps.setProperty(configNames, URLDecoder.decode(configVal, "UTF-8"));
                        } catch (UnsupportedEncodingException var21) {
                            urlProps.setProperty(configNames, URLDecoder.decode(configVal));
                        } catch (NoSuchMethodError var22) {
                            urlProps.setProperty(configNames, URLDecoder.decode(configVal));
                        }
                    }
                }
            }

            url = url.substring(beginningOfSlashes + 4);
            int slashIndex = url.indexOf("/");
            if (slashIndex != -1) {
                hostStuff = url.substring(0, slashIndex);
                if (slashIndex + 1 < url.length()) {
                    urlProps.put("dbname", url.substring(slashIndex + 1));
                }
            } else {
                hostStuff = url;
            }
            String hostAndPort;
            if (hostStuff != null && hostStuff.trim().length() > 0) {
                hostAndPort = hostStuff;
                String[] hostPortPair = parseHostPortPair(hostAndPort);
                if (hostPortPair.length != 2) {
                    return null;
                }
                if (hostPortPair[0] != null && hostPortPair[0].trim().length() > 0) {
                    urlProps.setProperty("remoteHost", hostPortPair[0]);
                }
                if (hostPortPair[1] != null) {
                    urlProps.setProperty("port", hostPortPair[1]);
                }
            }

            if (defaults != null) {
                urlProps.putAll(defaults);
            }
            return urlProps;
        }
    }

    @Override
    public Meta createMeta(AvaticaConnection connection) {

        // Create a single Service and set it on the Connection instance
        String host = props.getProperty("remoteHost");
        int port = Integer.parseInt(props.getProperty("port"));
        int timeout = 30;
        if (props.containsKey("timeout")) {
            timeout = Integer.parseInt(props.getProperty("timeout"));
        }

        Location location = new Location(host, port);
        Supplier<Location> locationSupplier = () -> location;

        NetConfiguration.resetAllTimeout(timeout);
        final DingoServiceImpl service = new DingoServiceImpl(locationSupplier, timeout);
        NetService.getDefault().newChannel(location).setCloseListener(NoBreakFunctions.wrap(ch -> {
            log.warn("Connection channel closed, close connection.");
            connection.close();
        }));
        connection.setService(service);
        return new DingoRemoteMeta(
            connection, service, ApiRegistry.getDefault().proxy(MetaApi.class, locationSupplier, timeout)
        );
    }
}
