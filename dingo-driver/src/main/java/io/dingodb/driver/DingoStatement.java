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

import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Helper;
import org.apache.calcite.avatica.Meta;

import java.sql.SQLException;

public class DingoStatement extends AvaticaStatement {
    DingoStatement(
        DingoConnection connection,
        Meta.StatementHandle handle,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability
    ) {
        super(
            connection,
            handle,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability
        );
    }

    public Meta.Signature getSignature() {
        return super.getSignature();
    }

    public void clear() throws SQLException {
        if (openResultSet != null) {
            final AvaticaResultSet rs = openResultSet;
            openResultSet = null;
            try {
                rs.close();
            } catch (Exception e) {
                throw Helper.INSTANCE.createException("Error while closing previous result set", e);
            }
        }
    }

    public void assign(Meta.Signature sig, Meta.Frame firstFrame, long uc, String sql) throws SQLException {
        setSignature(sig);
        updateCount = uc;
        // No result set for DDL.
        if (updateCount == -1) {
            openResultSet = ((DingoConnection) connection).newResultSet(this, sig, firstFrame, sql);
        }
    }
}
