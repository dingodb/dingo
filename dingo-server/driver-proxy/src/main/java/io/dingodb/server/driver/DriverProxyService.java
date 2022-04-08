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

package io.dingodb.server.driver;

import io.dingodb.driver.api.DriverProxyApi;
import io.dingodb.driver.server.ServerMetaFactory;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.remote.LocalService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

public class DriverProxyService extends LocalService implements DriverProxyApi {

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private RpcMetadataResponse serverLevelRpcMetadata;

    public DriverProxyService() throws Exception {
        super(new ServerMetaFactory().create(Collections.emptyList()), NoopMetricsSystem.getInstance());
    }

    @Override
    public void setRpcMetadata(RpcMetadataResponse serverLevelRpcMetadata) {
        super.setRpcMetadata(serverLevelRpcMetadata);
    }

    private static <E> List<E> list(Iterable<E> iterable) {
        if (iterable instanceof List) {
            return (List<E>) iterable;
        }
        final List<E> rowList = new ArrayList<>();
        for (E row : iterable) {
            rowList.add(row);
        }
        return rowList;
    }

    @Override
    public ResultSetResponse toResponse(Meta.MetaResultSet resultSet) {
        if (resultSet.updateCount != -1) {
            return new ResultSetResponse(resultSet.connectionId,
                resultSet.statementId, resultSet.ownStatement, null, null,
                resultSet.updateCount, serverLevelRpcMetadata);
        }

        Meta.Signature signature = resultSet.signature;

        // TODO Revise modification of CursorFactory see:
        // https://issues.apache.org/jira/browse/CALCITE-4567
        Meta.CursorFactory cursorFactory = resultSet.signature.cursorFactory;
        Meta.Frame frame = null;
        int updateCount = -1;
        final List<Object> list;

        if (resultSet.firstFrame != null) {
            list = list(resultSet.firstFrame.rows);
            final boolean done = resultSet.firstFrame.done;
            frame = new Meta.Frame(0, done, list);
            updateCount = -1;

            if (signature.statementType != null) {
                if (signature.statementType.canUpdate()) {
                    frame = null;
                    updateCount = ((Number) ((List) list.get(0)).get(0)).intValue();
                }
            }
        } else {
            cursorFactory = Meta.CursorFactory.LIST;
        }

        if (cursorFactory != resultSet.signature.cursorFactory) {
            signature = signature.setCursorFactory(cursorFactory);
        }

        return new ResultSetResponse(resultSet.connectionId, resultSet.statementId,
            resultSet.ownStatement, signature, frame, updateCount, serverLevelRpcMetadata);
    }

    public void start() {
        netService.apiRegistry().register(DriverProxyApi.class, this);
    }

    public void stop() {
        netService.apiRegistry().register(DriverProxyApi.class, null);
    }
}
