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

package io.dingodb.server.executor;

import io.dingodb.driver.ServerMeta;
import io.dingodb.driver.api.DriverProxyApi;
import io.dingodb.driver.api.MetaApi;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.api.ApiRegistry;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.remote.LocalService;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class DriverProxyServer extends LocalService implements DriverProxyApi, MetaApi {

    private static final Meta META = ServerMeta.getInstance();
    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private RpcMetadataResponse serverLevelRpcMetadata;

    public DriverProxyServer() {
        super(META, NoopMetricsSystem.getInstance());

        ApiRegistry.getDefault().register(DriverProxyApi.class, this);
        ApiRegistry.getDefault().register(MetaApi.class, this);
    }

    @Override
    public void setRpcMetadata(RpcMetadataResponse serverLevelRpcMetadata) {
        super.setRpcMetadata(serverLevelRpcMetadata);
    }

    @Override
    public Common.StatementHandle prepare(Meta.ConnectionHandle ch, String sql, long maxRowCount) {
        return META.prepare(ch, sql, maxRowCount).toProto();
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
            signature = signature.setCursorFactory(cursorFactory);
        }

        return new ResultSetResponse(resultSet.connectionId, resultSet.statementId,
            resultSet.ownStatement, signature, frame, updateCount, serverLevelRpcMetadata);
    }

    public void start() {
        ApiRegistry.getDefault().register(DriverProxyApi.class, this);
        ApiRegistry.getDefault().register(MetaApi.class, this);
        // ApiRegistry.getDefault().register(LogLevelApi.class, LogLevelApi.INSTANCE);
    }

    public void stop() {
        ApiRegistry.getDefault().register(DriverProxyApi.class, null);
        ApiRegistry.getDefault().register(MetaApi.class, null);
    }

    @Override
    public void cancel(String connectionId, int id, Meta.Signature signature) {
        ((ServerMeta)META).cancelStatement(connectionId, id, signature);
    }
}
