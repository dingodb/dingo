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

import io.dingodb.calcite.JobRunner;
import io.dingodb.exec.base.Job;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.TimeZone;

@Slf4j
public class DingoResultSet extends AvaticaResultSet {
    private Iterator<Object[]> iterator = null;

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

    Iterator<Object[]> getIterator() {
        if (iterator == null) {
            DingoSignature dingoSignature = (DingoSignature) signature;
            Job job = dingoSignature.getJob();
            Enumerator<Object[]> enumerator = new JobRunner(job).createEnumerator();
            iterator = Linq4j.enumeratorIterator(enumerator);
            try {
                setFetchSize(1);
            } catch (SQLException e) {
                e.printStackTrace();
                log.error("Executor Iterator catch exception:{}", e.toString(), e);
            }
        }
        return iterator;
    }
}
