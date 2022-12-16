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

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TokenManager {
    private static final long EXPIRE_DATE = 1000 * 10 * 60 * 24;

    static {
        INSTANCE = new TokenManager("0123456789");
    }

    public static final TokenManager INSTANCE;

    private String secret;

    // load from config file
    SignatureAlgorithm signatureAlgorithm;

    private TokenManager(String secret) {
        this(SignatureAlgorithm.HS256, secret);
    }

    private TokenManager(SignatureAlgorithm signatureAlgorithm, String secret) {
        this.secret = secret;
        this.signatureAlgorithm = signatureAlgorithm;
        // load secret from config token.pem
    }

    public String createInnerToken() {
        Map<String, Object> map = new HashMap<>();
        map.put("inner", "dingo");
        return createToken(map);
    }

    public String createToken(Map<String, Object> claimsMap) {
        Date date = new Date(System.currentTimeMillis() + EXPIRE_DATE);
        JwtBuilder builder = Jwts.builder();
        return builder.setSubject("dingo")
            .setClaims(claimsMap)
            .setExpiration(date)
            .signWith(SignatureAlgorithm.HS256, secret)
            .compact();
    }

    public Map<String, Object> certificateToken(String token) {
        if (token == null) {
            return null;
        }
        try {
            Jws<Claims> claimsJws
                = Jwts.parser()
                .setSigningKey(secret)
                .parseClaimsJws(token);
            Map claimsMap = claimsJws.getBody();
            return claimsMap;
        } catch (Exception e) {
            throw null;
        }
    }

}
