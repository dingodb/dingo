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

package io.dingodb.store.api.transaction.data.prewrite;

public enum PessimisticCheck {
    // The key needn't be locked, this is for optimistic transaction.
    SKIP_PESSIMISTIC_CHECK,
    // The key should have been locked at the time of prewrite, this is for pessimistic transaction.
    DO_PESSIMISTIC_CHECK
}
