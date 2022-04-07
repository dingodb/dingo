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

package io.dingodb.compare;

import io.dingodb.store.row.client.StoreRpcClient;
import io.dingodb.store.row.rpc.ApiCompareReply;
import io.dingodb.store.row.rpc.ApiGetLeaderReply;
import io.dingodb.store.row.rpc.ApiStatus;
import io.dingodb.store.row.rpc.CompareRegionApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompareRegion {
    private static Logger LOG = LoggerFactory.getLogger(CompareRegion.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java -jar io.dingodb.compare.CompareRegion\r\n"
                + "\t\t 172.20.3.66:19191 <regionId>");
            return;
        }

        String endpointString = args[0];
        String[] array = endpointString.split(":");
        if (array.length != 2) {
            System.out.println("ERROR: invalid endpoint: " + endpointString);
            return;
        }

        String ip = array[0];
        int port = Integer.parseInt(array[1]);
        LOG.info("Exector ip: {}, port: {}.", ip, port);

        String regionId = args[1];

        StoreRpcClient client = new StoreRpcClient();
        CompareRegionApi api = client.getCompareRegionApi(ip, port);
        ApiGetLeaderReply r1 = api.getLeader(regionId);
        if (r1.getStatus() != ApiStatus.OK) {
            LOG.error("===========> regionId: {}, get leader status: {}.", regionId, r1.getStatus().name());
            return;
        }
        LOG.info("Get leader status: {}.",  r1.getStatus().name());
        LOG.info("===========> regionId: {}, leader is {}:{}.", regionId, r1.getIp(), r1.getPort());
        CompareRegionApi api2 = client.getCompareRegionApi(r1.getIp(), r1.getPort());

        ApiStatus status = api2.startCompare(regionId);
        if (status != ApiStatus.OK) {
            if (status == ApiStatus.COMPARING_BY_ANOTHER) {
                LOG.warn("===========> regionId: {}, start compare status: {}.", regionId, status.name());
            } else {
                LOG.error("===========> regionId: {}, start compare status: {}.", regionId, status.name());
                return;
            }
        }

        while (true) {
            ApiCompareReply r2 = api2.getResult(regionId);
            if (r2.getStatus() == ApiStatus.COMPARING) {
                LOG.info("===========> regionId: {}, apiStatus: {}, msg: {}.", regionId, r2.getStatus().name(),
                    r2.getMsg());
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            } else {
                LOG.info("===========> regionId: {}, apiStatus: {}, compareResult: {}, msg: {}.", regionId,
                    r2.getStatus().name(), r2.getCompareResult(), r2.getMsg());
                break;
            }
        }
        LOG.info("exit main!!!");
    }
}
