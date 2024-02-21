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

package io.dingodb.exec.impl;

import io.dingodb.exec.transaction.visitor.data.CleanCacheLeaf;
import io.dingodb.exec.transaction.visitor.data.CommitLeaf;
import io.dingodb.exec.transaction.visitor.data.Composite;
import io.dingodb.exec.transaction.visitor.data.Leaf;
import io.dingodb.exec.transaction.visitor.data.PessimisticResidualLockLeaf;
import io.dingodb.exec.transaction.visitor.data.PessimisticRollBackScanLeaf;
import io.dingodb.exec.transaction.visitor.data.PreWriteLeaf;
import io.dingodb.exec.transaction.visitor.data.RollBackLeaf;
import io.dingodb.exec.transaction.visitor.data.PessimisticRollBackLeaf;
import io.dingodb.exec.transaction.visitor.data.RootLeaf;
import io.dingodb.exec.transaction.visitor.data.ScanCacheLeaf;
import io.dingodb.exec.transaction.visitor.data.ScanCacheResidualLockLeaf;
import io.dingodb.exec.transaction.visitor.data.ScanCleanCacheLeaf;
import io.dingodb.exec.transaction.visitor.data.StreamConverterLeaf;
import io.dingodb.exec.transaction.visitor.Visitor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestJobVisitor<T> implements Visitor<T> {

    public TestJobVisitor() {

    }

    @Override
    public T visit(Leaf leaf) {
        if(leaf.getData() != null)
            leaf.getData().accept(this);
        System.out.println("visitor Leaf：" + leaf.getName());
        return null;
    }

    @Override
    public T visit(RootLeaf rootLeaf) {
        if(rootLeaf.getData() != null)
            rootLeaf.getData().accept(this);
        System.out.println("visitor rootLeaf：" + rootLeaf.getName());
        return null;
    }

    @Override
    public T visit(ScanCacheLeaf scanCacheLeaf) {
        if(scanCacheLeaf.getData() != null)
            scanCacheLeaf.getData().accept(this);
        System.out.println("visitor ScanCacheLeaf：" + scanCacheLeaf.getName());
        return null;
    }

    @Override
    public T visit(PreWriteLeaf preWriteLeaf) {
        if(preWriteLeaf.getData() != null)
            preWriteLeaf.getData().accept(this);
        System.out.println("visitor preWriteLeaf：" + preWriteLeaf.getName());
        return null;
    }

    @Override
    public T visit(StreamConverterLeaf streamConverterLeaf) {
        if(streamConverterLeaf.getData() != null)
            streamConverterLeaf.getData().accept(this);
        System.out.println("visitor streamConverterLeaf：" + streamConverterLeaf.getName());
        return null;
    }

    @Override
    public T visit(CommitLeaf commitLeaf) {
        if(commitLeaf.getData() != null)
            commitLeaf.getData().accept(this);
        System.out.println("visitor commitLeaf：" + commitLeaf.getName());
        return null;
    }

    @Override
    public T visit(RollBackLeaf rollBackLeaf) {
        if(rollBackLeaf.getData() != null)
            rollBackLeaf.getData().accept(this);
        System.out.println("visitor rollBackLeaf：" + rollBackLeaf.getName());
        return null;
    }

    @Override
    public T visit(PessimisticRollBackLeaf pessimisticRollBackLeaf) {
        if(pessimisticRollBackLeaf.getData() != null)
            pessimisticRollBackLeaf.getData().accept(this);
        System.out.println("visitor rollBackPessimisticLockLeaf：" + pessimisticRollBackLeaf.getName());
        return null;
    }

    @Override
    public T visit(PessimisticRollBackScanLeaf pessimisticRollBackScanLeaf) {
        if(pessimisticRollBackScanLeaf.getData() != null)
            pessimisticRollBackScanLeaf.getData().accept(this);
        System.out.println("visitor pessimisticRollBackScanLeaf：" + pessimisticRollBackScanLeaf.getName());
        return null;
    }

    @Override
    public T visit(ScanCleanCacheLeaf scanCleanCacheLeaf) {
        if(scanCleanCacheLeaf.getData() != null)
            scanCleanCacheLeaf.getData().accept(this);
        System.out.println("visitor scanCleanCacheLeaf：" + scanCleanCacheLeaf.getName());
        return null;
    }

    @Override
    public T visit(PessimisticResidualLockLeaf pessimisticResidualLockLeaf) {
        if(pessimisticResidualLockLeaf.getData() != null)
            pessimisticResidualLockLeaf.getData().accept(this);
        System.out.println("visitor pessimisticResidualLockLeaf：" + pessimisticResidualLockLeaf.getName());
        return null;
    }

    @Override
    public T visit(ScanCacheResidualLockLeaf scanCacheResidualLockLeaf) {
        if(scanCacheResidualLockLeaf.getData() != null)
            scanCacheResidualLockLeaf.getData().accept(this);
        System.out.println("visitor scanCacheResidualLockLeaf：" + scanCacheResidualLockLeaf.getName());
        return null;
    }

    @Override
    public T visit(CleanCacheLeaf cleanCacheLeaf) {
        if(cleanCacheLeaf.getData() != null)
            cleanCacheLeaf.getData().accept(this);
        System.out.println("visitor cleanCacheLeaf：" + cleanCacheLeaf.getName());
        return null;
    }

    @Override
    public T visit(Composite composite) {
        System.out.println("visitor Composite：" + composite.getName());
        return null;
    }
}
