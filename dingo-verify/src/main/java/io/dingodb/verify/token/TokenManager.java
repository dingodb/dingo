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

package io.dingodb.verify.token;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.JWTVerifier;
import io.dingodb.common.config.CipherConfiguration;
import io.dingodb.common.config.SecurityConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class TokenManager {

    static {
        try {
            CipherConfiguration configuration = SecurityConfiguration.cipher();
            String jksPath = configuration.getKeyPath();
            String jksPassword = configuration.getKeyPass();
            String certAlias = configuration.getAlias();
            String certPassword = configuration.getStorePass();
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(new FileInputStream(jksPath), jksPassword.toCharArray());
            RSAPrivateKey privateKey = (RSAPrivateKey) keyStore.getKey(certAlias, certPassword.toCharArray());
            RSAPublicKey publicKey = (RSAPublicKey) keyStore.getCertificate(certAlias).getPublicKey();
            INSTANCE = new TokenManager(privateKey, publicKey, configuration.getIssuer());
        } catch (Exception e) {
            INSTANCE = new TokenManager(null, null, null);
        }
    }

    public static TokenManager INSTANCE;
    private RSAPrivateKey privateKey;
    private RSAPublicKey publicKey;

    private String issuer;

    private TokenManager(RSAPrivateKey privateKey, RSAPublicKey publicKey, String issuer) {
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        this.issuer = issuer;
    }

    public static TokenManager getInstance(String keyPath, String keyPass, String storePass,
                                           String alias, String issuer) {
        KeyStore keyStore = null;
        try {
            keyStore = KeyStore.getInstance("JKS");
            keyStore.load(new FileInputStream(keyPath), keyPass.toCharArray());
            RSAPrivateKey privateKey = (RSAPrivateKey) keyStore.getKey(alias, storePass.toCharArray());
            RSAPublicKey publicKey = (RSAPublicKey) keyStore.getCertificate(alias).getPublicKey();
            return new TokenManager(privateKey, publicKey, issuer);
        } catch (Exception e) {
            return new TokenManager(null, null, null);
        }
    }

    public String createInnerToken() {
        Map<String, Object> map = new HashMap<>();
        map.put("inner", "dingo");
        return createToken(map);
    }

    public String createToken(Map<String, Object> claimsMap) {
        try {
            Algorithm algorithm = Algorithm.RSA256(null, privateKey);
            return JWT.create()
                .withIssuer(issuer)
                .withPayload(claimsMap)
                .sign(algorithm);
        } catch (Exception e) {
            return null;
        }
    }

    public Map<String, Object> certificateToken(String token) {
        if (token == null) {
            return null;
        }
        DecodedJWT decodedJWT;
        try {
            Algorithm algorithm = Algorithm.RSA256(publicKey, null);
            JWTVerifier verifier = JWT.require(algorithm)
                .withIssuer(issuer)
                .build();

            decodedJWT = verifier.verify(token);
            Map<String, Claim> claimMap = decodedJWT.getClaims();
            Map<String, Object> resultMap = new HashMap<>();
            claimMap.entrySet().forEach(entry -> {
                String value = entry.getValue().toString();
                if (value.startsWith("\"")) {
                    resultMap.put(entry.getKey(), value.replace("\"", ""));
                } else {
                    resultMap.put(entry.getKey(), value);
                }
            });
            return resultMap;
        } catch (Exception e) {
            return null;
        }
    }

}
