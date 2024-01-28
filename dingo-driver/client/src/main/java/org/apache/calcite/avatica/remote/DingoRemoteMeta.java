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

package org.apache.calcite.avatica.remote;

import io.dingodb.common.util.Utils;
import io.dingodb.driver.DingoServiceImpl;
import io.dingodb.driver.api.MetaApi;
import lombok.SneakyThrows;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;

public class DingoRemoteMeta extends RemoteMeta {
    private final MetaApi metaApi;

    public DingoRemoteMeta(AvaticaConnection connection, DingoServiceImpl service, MetaApi metaApi) {
        super(connection, service);
        this.metaApi = metaApi;
    }

    @Override
    @SneakyThrows
    public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
        try {
            return StatementHandle.fromProto(metaApi.prepare(ch, sql, maxRowCount));
        } catch (Exception e) {
            throw Utils.extractThrowable(e);
        }
    }

    @Override
    public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
        return connection.invokeWithRetries(
            () -> {
                ConnectionPropertiesImpl localProps = propsMap.get(ch.id);
                if (localProps == null) {
                    localProps = new ConnectionPropertiesImpl();
                    localProps.setDirty(true);
                    propsMap.put(ch.id, localProps);
                }

                // Only make an RPC if necessary. RPC is necessary when we have local changes that need
                // flushed to the server (be sure to introduce any new changes from connProps before
                // checking AND when connProps.isEmpty() (meaning, this was a request for a value, not
                // overriding a value). Otherwise, accumulate the change locally and return immediately.
                if (localProps.merge(connProps).isDirty()) {
                    final Service.ConnectionSyncResponse response = service.apply(
                        new Service.ConnectionSyncRequest(ch.id, localProps));
                    propsMap.put(ch.id, (ConnectionPropertiesImpl) response.connProps);
                    return response.connProps;
                } else {
                    return localProps;
                }
            });
    }
}
