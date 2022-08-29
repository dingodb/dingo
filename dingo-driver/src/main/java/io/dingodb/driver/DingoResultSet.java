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

import com.google.common.collect.ImmutableList;
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
import javax.annotation.Nullable;

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
        return getIterator(null);
    }

    Iterator<Object[]> getIterator(@Nullable Object[] paras) {
        if (iterator == null) {
            if (signature instanceof DingoExplainSignature) {
                DingoExplainSignature signature = (DingoExplainSignature) this.signature;
                return ImmutableList.of(new Object[]{signature.toString()}).iterator();
            }
            DingoSignature dingoSignature = (DingoSignature) signature;
            Job job = dingoSignature.getJob();
            if (paras != null) {
                job.setParas(paras);
            }
            Enumerator<Object[]> enumerator = new JobRunner(job).createEnumerator();
            iterator = Linq4j.enumeratorIterator(enumerator);
            try {
                setFetchSize(100);
            } catch (SQLException e) {
                log.error("Executor Iterator catch exception:{}", e.getMessage(), e);
            }
        }
        return iterator;
    }
}
