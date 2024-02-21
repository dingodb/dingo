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

package io.dingodb.exec.transaction.visitor.data;

public class ElementName {

    public static final String ROOT = "Root";
    public static final String SCAN_CACHE = "ScanCache";
    public static final String PRE_WRITE = "PreWrite";
    public static final String COMMIT = "Commit";
    public static final String COMMIT_PRIMARY_KEY = "CommitPrimaryKey";
    public static final String STREAM = "StreamConverter";
    public static final String ROLLBACK = "RollBack";
    public static final String CLEAN_CACHE = "CleanCache";

    public static final String PESSIMISTIC_ROLLBACK = "PessimisticRollBack";

    public static final String PESSIMISTIC_ROLLBACK_SCAN = "PessimisticRollBackScan";

    public static final String PESSIMISTIC_RESIDUAL_LOCK= "PessimisticResidualLock";

    public static final String SINGLE_TRANSACTION_PRE_WRITE = "SingleTransactionPreWrite";
    public static final String MULTI_TRANSACTION_PRE_WRITE = "MultiTransactionPreWrite";

    public static final String SINGLE_TRANSACTION_COMMIT = "SingleTransactionCommit";

    public static final String MULTI_TRANSACTION_COMMIT = "MultiTransactionCommit";

    public static final String SINGLE_TRANSACTION_ROLLBACK = "SingleTransactionRollBack";

    public static final String MULTI_TRANSACTION_ROLLBACK = "MultiTransactionRollBack";
    public static final String SINGLE_TRANSACTION_PESSIMISTIC_ROLLBACK = "SingleTransactionPessimisticRollBack";
    public static final String MULTI_TRANSACTION_PESSIMISTIC_ROLLBACK = "MultiTransactionPessimisticRollBack";

    public static final String SINGLE_TRANSACTION_RESIDUAL_LOCK = "SingleTransactionResidualLock";
    public static final String MULTI_TRANSACTION_RESIDUAL_LOCK = "MultiTransactionResidualLock";

    public static final String SINGLE_TRANSACTION_CLEAN_CACHE = "SingleTransactionCleanCache";
    public static final String MULTI_TRANSACTION_CLEAN_CACHE = "MultiTransactionCleanCache";
}
