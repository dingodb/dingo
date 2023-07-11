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

package io.dingodb.driver;

import io.dingodb.exec.base.JobIterator;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.TimeZone;

@Slf4j
public class DingoResultSet extends AvaticaResultSet {
    @Getter
    @Setter
    private Iterator<Object[]> iterator;

    public DingoResultSet(
        AvaticaStatement statement,
        QueryState state,
        Meta.Signature signature,
        ResultSetMetaData resultSetMetaData,
        TimeZone timeZone,
        Meta.Frame firstFrame
    ) throws SQLException {
        super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
    }

    @Override
    protected void cancel() {
        if (iterator instanceof JobIterator) {
            ((JobIterator) iterator).cancel();
        }
    }

    public Meta.Signature getSignature() {
        return signature;
    }
}
