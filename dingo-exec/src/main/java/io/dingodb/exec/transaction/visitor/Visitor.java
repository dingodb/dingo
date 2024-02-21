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

package io.dingodb.exec.transaction.visitor;

import io.dingodb.exec.transaction.visitor.data.CleanCacheLeaf;
import io.dingodb.exec.transaction.visitor.data.CommitLeaf;
import io.dingodb.exec.transaction.visitor.data.Composite;
import io.dingodb.exec.transaction.visitor.data.Leaf;
import io.dingodb.exec.transaction.visitor.data.PessimisticResidualLockLeaf;
import io.dingodb.exec.transaction.visitor.data.PessimisticRollBackScanLeaf;
import io.dingodb.exec.transaction.visitor.data.PreWriteLeaf;
import io.dingodb.exec.transaction.visitor.data.PessimisticRollBackLeaf;
import io.dingodb.exec.transaction.visitor.data.RollBackLeaf;
import io.dingodb.exec.transaction.visitor.data.RootLeaf;
import io.dingodb.exec.transaction.visitor.data.ScanCacheLeaf;
import io.dingodb.exec.transaction.visitor.data.ScanCacheResidualLockLeaf;
import io.dingodb.exec.transaction.visitor.data.ScanCleanCacheLeaf;
import io.dingodb.exec.transaction.visitor.data.StreamConverterLeaf;

public interface Visitor<T> {
    T visit(Leaf leaf);

    T visit(RootLeaf rootLeaf);

    T visit(ScanCacheLeaf scanCacheLeaf);

    T visit(PreWriteLeaf preWriteLeaf);

    T visit(StreamConverterLeaf streamConverterLeaf);

    T visit(CommitLeaf commitLeaf);

    T visit(RollBackLeaf rollBackLeaf);

    T visit(PessimisticRollBackLeaf pessimisticRollBackLeaf);

    T visit(PessimisticRollBackScanLeaf pessimisticRollBackScanLeaf);

    T visit(ScanCleanCacheLeaf scanCleanCacheLeaf);

    T visit(PessimisticResidualLockLeaf pessimisticResidualLockLeaf);

    T visit(ScanCacheResidualLockLeaf scanCacheResidualLockLeaf);

    T visit(CleanCacheLeaf cleanCacheLeaf);

    T visit(Composite composite);
}
