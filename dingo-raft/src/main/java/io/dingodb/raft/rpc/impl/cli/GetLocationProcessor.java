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

package io.dingodb.raft.rpc.impl.cli;

import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.raft.rpc.RpcContext;
import io.dingodb.raft.rpc.RpcProcessor;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

public class GetLocationProcessor implements RpcProcessor<GetLocationProcessor.GetLocationRequest> {

    @Override
    public void handleRequest(RpcContext rpcCtx, GetLocationRequest request) {
        rpcCtx.sendResponse(GetLocationResponse.INSTANCE);
    }

    @Override
    public String interest() {
        return GetLocationRequest.class.getName();
    }

    public static class GetLocationRequest implements Serializable {
        public static final GetLocationRequest INSTANCE = new GetLocationRequest();
        private static final long serialVersionUID = 3330184876176689500L;
    }

    @Getter
    @ToString
    @AllArgsConstructor
    public static class GetLocationResponse implements Serializable {
        private static final long serialVersionUID = 3104200417193071495L;
        public static final GetLocationResponse INSTANCE = new GetLocationResponse();
        private final Location location = new Location(DingoConfiguration.host(), DingoConfiguration.port());
    }
}
