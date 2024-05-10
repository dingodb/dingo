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

package io.dingodb.driver.mysql.netty;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchScope;
import io.dingodb.common.config.CipherConfiguration;
import io.dingodb.common.config.LdapConfiguration;
import io.dingodb.common.config.SecurityConfiguration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class LdapConn {
    static String ldapHost;
    static Integer ldapPort;
    static String bindDN;
    static String password;

    static String baseDN;

    static {
        try {
            LdapConfiguration configuration = SecurityConfiguration.ldap();
            ldapHost = configuration.getLdapHost();
            ldapPort = configuration.getLdapPort();
            bindDN = configuration.getBindDN();
            password = configuration.getPassword();
            baseDN = configuration.getBaseDN();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private LdapConn() {
    }

    public static boolean conn(String userName, String pwd) {
        //String ldapHost = "ldap";
        //int ldapPort = 389;
        //String bindDN = "cn=admin,dc=localdomain,dc=com";
        //String password = "123456";

        try (LDAPConnection connection = new LDAPConnection(ldapHost, ldapPort, bindDN, password)) {
            SearchResult searchResult = connection.search(baseDN, SearchScope.SUB, "uid=" + userName);
            if (searchResult.getEntryCount() == 1) {
                String userDn = searchResult.getSearchEntries().get(0).getDN();
                try (LDAPConnection userConn = new LDAPConnection(ldapHost, ldapPort, userDn, pwd)) {
                    return userConn.isConnected();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        } catch (LDAPException e) {
            log.error("login failed: " + e.getMessage());
        }
        return false;
    }
}
