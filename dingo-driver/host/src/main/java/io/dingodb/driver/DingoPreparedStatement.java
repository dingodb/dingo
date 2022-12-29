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

import io.dingodb.driver.type.converter.TypedValueConverter;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class DingoPreparedStatement extends AvaticaPreparedStatement {
    protected DingoPreparedStatement(
        DingoConnection connection,
        Meta.StatementHandle handle,
        Meta.Signature signature,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) throws SQLException {
        super(
            connection,
            handle,
            signature,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability
        );
    }

    void setParameterValues(@NonNull List<TypedValue> parameterValues) {
        for (int i = 0; i < parameterValues.size(); ++i) {
            slots[i] = parameterValues.get(i);
        }
    }

    @Override
    protected void setSignature(Meta.Signature signature) {
        super.setSignature(signature);
    }

    void createResultSet(Meta.@Nullable Frame firstFrame) throws SQLException {
        if (openResultSet != null) {
            openResultSet.close();
        }
        Meta.Signature signature = getSignature();
        openResultSet = ((DingoConnection) connection).newResultSet(
            this,
            signature,
            firstFrame,
            signature.sql
        );
    }

    @NonNull
    public Iterator<Object[]> createIterator(@NonNull JobManager jobManager) {
        Meta.Signature signature = getSignature();
        if (signature instanceof DingoSignature) {
            try {
                Object[] parasValue = TypedValue.values(getParameterValues()).toArray();
                Id jobId = ((DingoSignature) signature).getJobId();
                Job job = jobManager.getJob(jobId);
                Object[] paras = ((Object[]) job.getParasType().convertFrom(
                    parasValue,
                    new TypedValueConverter(getCalendar())
                ));
                return jobManager.createIterator(job, paras);
            } catch (NullPointerException e) {
                throw new IllegalStateException("Not all parameters are set.");
            }
        }
        throw ExceptionUtils.wrongSignatureType(this, signature);
    }
}
