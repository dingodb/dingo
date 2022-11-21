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

package io.dingodb.verify.privilege;

import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.UserDefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrivilegeVerify {

    private static Map<PrivilegeType, PrivilegeVerify> privilegeVerifyMap = new HashMap<>();

    static {
        privilegeVerifyMap.put(PrivilegeType.SQL, new DriverPrivilegeVerify());
        privilegeVerifyMap.put(PrivilegeType.SDK, new SdkClientPrivilegeVerify());
        privilegeVerifyMap.put(PrivilegeType.API, new ApiPrivilegeVerify());
    }

    public static String getUserHost() {
        return "";
    }

    public static boolean isVerify = false;

    public PrivilegeVerify() {
        this(true);
    }

    public PrivilegeVerify(boolean isVerify) {
        this.isVerify = isVerify;
    }

    public UserDefinition matchUser(String host, PrivilegeGather privilegeGather) {
        //fake data
        UserDefinition userDef = new UserDefinition();
        userDef.setUsername("root");
        userDef.setHost("172.20.3.93");
        userDef.setPassword("cbcce4ebcf0e63f32a3d6904397792720f7e40ba");
        userDef.setPlugin("mysql_native_password");
        return userDef;
    }

    public UserDefinition matchUser(String host, List<UserDefinition> userDefList) {
        //fake data
        // todo
        if (userDefList != null && userDefList.size() > 0) {
            return userDefList.get(0);
        } else {
            return null;
        }
    }

    public boolean verify(Object... param) {
        return false;
    }

    /**
     * privilege verify.
     * @param verifyType driver/sdk/api
     * @param param param 0: user,host  1: privilege 2: privilege select/update/delete/insert
     * @return true/false
     */
    public boolean verify(PrivilegeType verifyType, Object... param) {
        if (isVerify) {
            PrivilegeVerify privilegeVerify = privilegeVerifyMap.get(verifyType);
            return privilegeVerify.verify(param);
        } else {
            return true;
        }
    }
}
