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

package io.dingodb.exec.transaction.base;

/**
 * Transaction Status
 */
public enum TransactionStatus {
    UNKNOWN, START, RUNNING, PRE_WRITE_START, PRE_WRITE_PRIMARY_KEY, PRE_WRITE, PRE_WRITE_RETRY, PRE_WRITE_FAIL, COMMIT,
    COMMIT_PRIMARY_KEY, COMMIT_FAIL, ROLLBACK, ROLLBACK_FAIL, ROLLBACK_PESSIMISTIC_LOCK, ROLLBACK_PESSIMISTIC_LOCK_FAIL,
    ROLLBACK_OPTIMISTIC_DATA_START, ROLLBACK_OPTIMISTIC_DATA, ROLLBACK_OPTIMISTIC_DATA_FAIL, CLEAN_OPTIMISTIC_DATA_START,
    CLEAN_OPTIMISTIC_DATA, CLEAN_OPTIMISTIC_DATA_FAIL, ROLLBACK_RESIDUAL_PESSIMISTIC_LOCK,
    ROLLBACK_RESIDUAL_PESSIMISTIC_LOCK_FAIL, CLOSE, CANCEL, ONE_PC_START
}
