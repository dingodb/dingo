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

package io.dingodb.raft.util;

import lombok.extern.slf4j.Slf4j;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Slf4j
public class Md5 {
    /**
     * @param str
     * @return
     * @Description: 32bit lower-case MD5
     */
    public static String parseStrToMd5L32(String str){
        String retStr = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bytes = md5.digest(str.getBytes());
            StringBuffer stringBuffer = new StringBuffer();
            for (byte b : bytes){
                int bt = b&0xff;
                if (bt < 16){
                    stringBuffer.append(0);
                }
                stringBuffer.append(Integer.toHexString(bt));
            }
            retStr = stringBuffer.toString();
        } catch (NoSuchAlgorithmException e) {
            log.error("Not found MD5.", e);
        }
        return retStr;
    }

    /**
     * @param str
     * @return
     * @Description: 32bits upper-case MD5
     */
    public static String parseStrToMd5U32(String str){
        String retStr = parseStrToMd5L32(str);
        if (retStr != null){
            retStr = retStr.toUpperCase();
        }
        return retStr;
    }

    /**
     * @param str
     * @return
     * @Description: 16bits lower-case MD5
     */
    public static String parseStrToMd5L16(String str){
        String retStr = parseStrToMd5L32(str);
        if (retStr != null){
            retStr = retStr.substring(16, 32);
        }
        return retStr;
    }

    /**
     * @param str
     * @return
     * @Description: 16bits upper-case MD5
     */
    public static String parseStrToMd5U16(String str){
        String retStr = parseStrToMd5L32(str);
        if (retStr != null){
            retStr = retStr.toUpperCase().substring(16, 32);
        }
        return retStr;
    }
}
