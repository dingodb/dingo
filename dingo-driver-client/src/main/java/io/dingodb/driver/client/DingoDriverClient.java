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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.function.Supplier;

@Slf4j
public class DingoDriverClient extends Driver {
    public static final String CONNECT_STRING_PREFIX = "jdbc:dingo:thin:";

    private Properties props;

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

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if ((props = this.parseURL(url, info)) == null) {
            throw new IllegalArgumentException("Bad url: " + url);
        } else {
            log.info("info:" + props);
            return super.connect(url, info);
        }
    }

    private Properties parseURL(String url, Properties defaults) throws SQLException {
        Properties urlProps = defaults != null ? new Properties(defaults) : new Properties();
        if (url == null) {
            return null;
        } else if (!startsWithIgnoreCase(url, CONNECT_STRING_PREFIX)) {
            return null;
        } else {
            int beginningOfSlashes = url.indexOf("url=");
            int index = url.indexOf("?");
            String hostStuff;
            String configNames;
            String configVal;
            if (index != -1) {
                hostStuff = url.substring(index + 1, url.length());
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
                    urlProps.put("dbname", url.substring(slashIndex + 1, url.length()));
                }
            } else {
                hostStuff = url;
            }
            String hostAndPort;
            if (hostStuff != null && hostStuff.trim().length() > 0) {
                hostAndPort = hostStuff;
                String[] hostPortPair = parseHostPortPair(hostAndPort);
                if (hostPortPair.length != 2) {
                    throw new IllegalArgumentException("Bad url: " + url);
                }
                if (hostPortPair[0] != null && hostPortPair[0].trim().length() > 0) {
                    urlProps.setProperty("host", hostPortPair[0]);
                }
                if (hostPortPair[1] != null) {
                    urlProps.setProperty("port", hostPortPair[1]);
                }
            }

            if (defaults != null) {
                urlProps.putAll(urlProps);
            }
            return urlProps;
        }
    }

    protected static String[] parseHostPortPair(String hostPortPair) throws SQLException {
        String[] splitValues = new String[2];
        int portIndex = hostPortPair.indexOf(":");
        String hostname = null;
        if (portIndex != -1) {
            if (portIndex + 1 >= hostPortPair.length()) {
                throw new SQLException("xx");
            }

            String portAsString = hostPortPair.substring(portIndex + 1);
            hostname = hostPortPair.substring(0, portIndex);
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

    @Override public Meta createMeta(AvaticaConnection connection) {
        final ConnectionConfig config = connection.config();

        // Create a single Service and set it on the Connection instance
        String host = props.getProperty("host");
        int port = Integer.parseInt(props.getProperty("port"));
        int timeout = 30;
        if (props.containsKey("timeout")) {
            timeout = Integer.parseInt(props.getProperty("timeout"));
        }

        Location location = new Location(host, port);
        Supplier<Location> locationSupplier = () -> location;

        final Service service = new DingoServiceImpl(locationSupplier, timeout);
        connection.setService(service);
        return new DingoRemoteMeta(connection, service);
    }
}
