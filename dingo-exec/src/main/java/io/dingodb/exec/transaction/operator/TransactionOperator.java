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

package io.dingodb.exec.transaction.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.Services;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.SoleOutOperator;
import io.dingodb.exec.table.Part;
import io.dingodb.exec.table.PartInKvStore;
import lombok.Getter;
import lombok.Setter;
import org.checkerframework.checker.nullness.qual.Nullable;

@JsonTypeName("transaction")
public class TransactionOperator extends SoleOutOperator {

    @JsonProperty("schema")
    @Getter
    protected final DingoType schema;

    @Getter
    @Setter
    protected long txn_size;

    protected Part part;

    @JsonCreator
    protected TransactionOperator(@JsonProperty("schema") DingoType schema) {
        super();
        this.schema = schema;
    }

    @Override
    public void init() {
        super.init();
//        part = new PartInKvStore(
//            Services.KV_STORE.getInstance(new CommonId(CommonId.CommonType.TABLE, 2, 74438), new CommonId(CommonId.CommonType.DISTRIBUTION, 74440,86127)),
//            null
//        );
    }

    @Override
    public boolean push(int pin, @Nullable Object[] tuple) {
        return false;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin) {

    }
}
