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

package io.dingodb.driver.mysql.facotry;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public class SecureChatSslContextFactory {
    private static final String PROTOCOL = "TLSv1.2";

    private static SSLContext SERVER_CONTEXT;

    private static SSLContext CLIENT_CONTEXT;

    public static SSLContext getServerContext() {
        if (SERVER_CONTEXT != null) {
            return SERVER_CONTEXT;
        }
        InputStream in = null;

        try {
            KeyManagerFactory kmf = null;

            KeyStore ks = KeyStore.getInstance("JKS");

            ks.load(SecureChatSslContextFactory.class.getClassLoader().getResourceAsStream("sChat.jks"),
                "sNetty".toCharArray());

            kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, "sNetty".toCharArray());

            SERVER_CONTEXT = SSLContext.getInstance(PROTOCOL);
            // param1: authed key
            // param2:: auth client cert
            SERVER_CONTEXT.init(kmf.getKeyManagers(), null, null);

        } catch (Exception e) {
            throw new Error("Failed to initialize the server-side SSLContext", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
        return SERVER_CONTEXT;
    }


    public static SSLContext getClientContext(String caPath) {
        if (CLIENT_CONTEXT != null) {
            return CLIENT_CONTEXT;
        }
        InputStream tIN = null;
        try {
            TrustManagerFactory tf = null;
            if (caPath != null) {
                KeyStore tks = KeyStore.getInstance("JKS");
                tIN = new FileInputStream(caPath);
                tks.load(tIN, "sNetty".toCharArray());
                tf = TrustManagerFactory.getInstance("SunX509");
                tf.init(tks);
            }

            CLIENT_CONTEXT = SSLContext.getInstance(PROTOCOL);
            CLIENT_CONTEXT.init(null, tf == null ? null : tf.getTrustManagers(), null);

        } catch (Exception e) {
            e.printStackTrace();
            throw new Error("Failed to initialize the client-side SSLContext");
        } finally {
            if (tIN != null) {
                try {
                    tIN.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return CLIENT_CONTEXT;
    }
}
