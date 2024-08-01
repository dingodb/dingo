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

package io.dingodb.common.environment;

import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.ddl.MdlCheckTableInfo;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.session.SessionUtil;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class ExecutionEnvironment {

    public static ExecutionEnvironment INSTANCE = new ExecutionEnvironment();

    public SessionUtil sessionUtil = SessionUtil.INSTANCE;

    public ReentrantLock lock = new ReentrantLock();

    public AtomicBoolean ddlOwner = new AtomicBoolean(false);

    public LocalMemCacheFor2PC memCacheFor2PC = new LocalMemCacheFor2PC();

    public MdlCheckTableInfo mdlCheckTableInfo = new MdlCheckTableInfo();

    public ClientIdentity clientIdentity = new ClientIdentity();

    @Getter
    @Setter
    private DingoRole role;

    @Setter
    @Getter
    private volatile Map<String, PrivilegeGather> privilegeGatherMap = new ConcurrentHashMap<>();

}
