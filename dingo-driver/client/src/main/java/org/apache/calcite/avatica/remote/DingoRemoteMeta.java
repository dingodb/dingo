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

import io.dingodb.driver.DingoServiceImpl;
import io.dingodb.driver.api.MetaApi;
import org.apache.calcite.avatica.AvaticaConnection;

public class DingoRemoteMeta extends RemoteMeta {
    private final MetaApi metaApi;

    public DingoRemoteMeta(AvaticaConnection connection, DingoServiceImpl service, MetaApi metaApi) {
        super(connection, service);
        this.metaApi = metaApi;
    }

    @Override
    public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
        return StatementHandle.fromProto(metaApi.prepare(ch, sql, maxRowCount));
    }
}
