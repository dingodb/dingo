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

package io.dingodb.verify.plugin;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;

@Slf4j
public class MysqlNativePassword extends AlgorithmPlugin {

    @Override
    public String getEncodedPassword(String password) {
        try {
            if (StringUtils.isEmpty(password)) {
                return "";
            }
            return scramble(password);
        } catch (UnsupportedEncodingException e) {
            log.error("Encoded password error : {}", e.getMessage(), e);
        }
        return null;
    }

    private static String scramble(String password) throws UnsupportedEncodingException {
        MessageDigest sha = null;
        try {
            sha = MessageDigest.getInstance("SHA-1");
        } catch (Exception e) {
            return "";
        }
        byte[] passwordHashStage1 = sha.digest(password.getBytes());
        sha.reset();

        byte[] passwordHashStage2 = sha.digest(passwordHashStage1);

        StringBuffer hexValue = new StringBuffer();
        for (int i = 0; i < passwordHashStage2.length; i++) {
            int val = ((int) passwordHashStage2[i]) & 0xff;
            if (val < 16) {
                hexValue.append("0");
            }
            hexValue.append(Integer.toHexString(val));
        }
        return hexValue.toString();
    }
}
