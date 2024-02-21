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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionElements {
    private static Map<String, Element> elementMap = new ConcurrentHashMap<>();

    static {
        elementMap.put(ElementName.SINGLE_TRANSACTION_PRE_WRITE, createSingleTransactionPreWrite());
        elementMap.put(ElementName.SINGLE_TRANSACTION_COMMIT, createSingleTransactionCommit());
        elementMap.put(ElementName.SINGLE_TRANSACTION_ROLLBACK, createSingleTransactionRollBack());
        elementMap.put(ElementName.SINGLE_TRANSACTION_PESSIMISTIC_ROLLBACK, createSingleTransactionRollBackPl());
        elementMap.put(ElementName.SINGLE_TRANSACTION_CLEAN_CACHE, createSingleTransactionCleanCache());
        elementMap.put(ElementName.SINGLE_TRANSACTION_RESIDUAL_LOCK, createSingleTransactionResidualLock());
        elementMap.put(ElementName.MULTI_TRANSACTION_PRE_WRITE, createMultiTransactionPreWrite());
        elementMap.put(ElementName.MULTI_TRANSACTION_COMMIT, createMultiTransactionCommit());
        elementMap.put(ElementName.MULTI_TRANSACTION_ROLLBACK, createMultiTransactionRollBack());
        elementMap.put(ElementName.MULTI_TRANSACTION_PESSIMISTIC_ROLLBACK, createMultiTransactionRollBackPl());
        elementMap.put(ElementName.MULTI_TRANSACTION_CLEAN_CACHE, createMultiTransactionCleanCache());
        elementMap.put(ElementName.MULTI_TRANSACTION_RESIDUAL_LOCK, createMultiTransactionResidualLock());
    }

    private static Element createSingleTransactionPreWrite() {
        ScanCacheLeaf scanCacheLeaf = ScanCacheLeaf.builder()
            .name(ElementName.SCAN_CACHE)
            .build();
        PreWriteLeaf preWriteLeaf = PreWriteLeaf.builder()
            .name(ElementName.PRE_WRITE)
            .data(scanCacheLeaf)
            .build();
        RootLeaf root = RootLeaf.builder()
            .name(ElementName.ROOT)
            .data(preWriteLeaf)
            .build();
        return root;
    }

    private static Element createSingleTransactionCommit() {
        ScanCacheLeaf scanCacheLeaf = ScanCacheLeaf.builder()
            .name(ElementName.SCAN_CACHE)
            .build();
        CommitLeaf commitLeaf = CommitLeaf.builder()
            .name(ElementName.COMMIT)
            .data(scanCacheLeaf)
            .build();
        RootLeaf root = RootLeaf.builder()
            .name(ElementName.ROOT)
            .data(commitLeaf)
            .build();
        return root;
    }

    private static Element createSingleTransactionRollBack() {
        ScanCacheLeaf scanCacheLeaf = ScanCacheLeaf.builder()
            .name(ElementName.SCAN_CACHE)
            .build();
        RollBackLeaf rollBackLeaf = RollBackLeaf.builder()
            .name(ElementName.ROLLBACK)
            .data(scanCacheLeaf)
            .build();
        RootLeaf root = RootLeaf.builder()
            .name(ElementName.ROOT)
            .data(rollBackLeaf)
            .build();
        return root;
    }

    private static Element createMultiTransactionPreWrite() {
        ScanCacheLeaf scanCacheLeaf = ScanCacheLeaf.builder()
            .name(ElementName.SCAN_CACHE)
            .build();
        PreWriteLeaf preWriteLeaf = PreWriteLeaf.builder()
            .name(ElementName.PRE_WRITE)
            .data(scanCacheLeaf)
            .build();
        StreamConverterLeaf streamConverterLeaf = StreamConverterLeaf.builder()
            .name(ElementName.STREAM)
            .data(preWriteLeaf)
            .build();
        RootLeaf root = RootLeaf.builder()
            .name(ElementName.ROOT)
            .data(streamConverterLeaf)
            .build();
        return root;
    }

    private static Element createMultiTransactionCommit() {
        ScanCacheLeaf scanCacheLeaf = ScanCacheLeaf.builder()
            .name(ElementName.SCAN_CACHE)
            .build();
        CommitLeaf commitLeaf = CommitLeaf.builder()
            .name(ElementName.COMMIT)
            .data(scanCacheLeaf)
            .build();
        StreamConverterLeaf streamConverterLeaf = StreamConverterLeaf.builder()
            .name(ElementName.STREAM)
            .data(commitLeaf)
            .build();
        RootLeaf root = RootLeaf.builder()
            .name(ElementName.ROOT)
            .data(streamConverterLeaf)
            .build();
        return root;
    }

    private static Element createMultiTransactionRollBack() {
        ScanCacheLeaf scanCacheLeaf = ScanCacheLeaf.builder()
            .name(ElementName.SCAN_CACHE)
            .build();
        RollBackLeaf rollBackLeaf = RollBackLeaf.builder()
            .name(ElementName.ROLLBACK)
            .data(scanCacheLeaf)
            .build();
        StreamConverterLeaf streamConverterLeaf = StreamConverterLeaf.builder()
            .name(ElementName.STREAM)
            .data(rollBackLeaf)
            .build();
        RootLeaf root = RootLeaf.builder()
            .name(ElementName.ROOT)
            .data(streamConverterLeaf)
            .build();
        return root;
    }

    private static Element createSingleTransactionRollBackPl() {
        PessimisticRollBackScanLeaf pessimisticRollBackScanLeaf = PessimisticRollBackScanLeaf.builder()
            .name(ElementName.PESSIMISTIC_ROLLBACK_SCAN)
            .build();
        PessimisticRollBackLeaf pessimisticRollBackLeaf = PessimisticRollBackLeaf.builder()
            .name(ElementName.PESSIMISTIC_ROLLBACK)
            .data(pessimisticRollBackScanLeaf)
            .build();
        RootLeaf root = RootLeaf.builder()
            .name(ElementName.ROOT)
            .data(pessimisticRollBackLeaf)
            .build();
        return root;
    }

    private static Element createMultiTransactionRollBackPl() {
        PessimisticRollBackScanLeaf pessimisticRollBackScanLeaf = PessimisticRollBackScanLeaf.builder()
            .name(ElementName.PESSIMISTIC_ROLLBACK_SCAN)
            .build();
        PessimisticRollBackLeaf pessimisticRollBackLeaf = PessimisticRollBackLeaf.builder()
            .name(ElementName.PESSIMISTIC_ROLLBACK)
            .data(pessimisticRollBackScanLeaf)
            .build();
        StreamConverterLeaf streamConverterLeaf = StreamConverterLeaf.builder()
            .name(ElementName.STREAM)
            .data(pessimisticRollBackLeaf)
            .build();
        RootLeaf root = RootLeaf.builder()
            .name(ElementName.ROOT)
            .data(streamConverterLeaf)
            .build();
        return root;
    }

    private static Element createMultiTransactionCleanCache() {
        ScanCleanCacheLeaf scanCleanCacheLeaf = ScanCleanCacheLeaf.builder()
            .name(ElementName.SCAN_CACHE)
            .build();
        CleanCacheLeaf cleanCacheLeaf = CleanCacheLeaf.builder()
            .name(ElementName.CLEAN_CACHE)
            .data(scanCleanCacheLeaf)
            .build();
        StreamConverterLeaf streamConverterLeaf = StreamConverterLeaf.builder()
            .name(ElementName.STREAM)
            .data(cleanCacheLeaf)
            .build();
        RootLeaf root = RootLeaf.builder()
            .name(ElementName.ROOT)
            .data(streamConverterLeaf)
            .build();
        return root;
    }

    private static Element createSingleTransactionCleanCache() {
        ScanCleanCacheLeaf scanCleanCacheLeaf = ScanCleanCacheLeaf.builder()
            .name(ElementName.SCAN_CACHE)
            .build();
        CleanCacheLeaf cleanCacheLeaf = CleanCacheLeaf.builder()
            .name(ElementName.CLEAN_CACHE)
            .data(scanCleanCacheLeaf)
            .build();
        RootLeaf root = RootLeaf.builder()
            .name(ElementName.ROOT)
            .data(cleanCacheLeaf)
            .build();
        return root;
    }

    private static Element createSingleTransactionResidualLock() {
        ScanCacheResidualLockLeaf scanCacheResidualLockLeaf = ScanCacheResidualLockLeaf.builder()
            .name(ElementName.SCAN_CACHE)
            .build();
        PessimisticResidualLockLeaf pessimisticResidualLockLeaf = PessimisticResidualLockLeaf.builder()
            .name(ElementName.PESSIMISTIC_RESIDUAL_LOCK)
            .data(scanCacheResidualLockLeaf)
            .build();
        RootLeaf root = RootLeaf.builder()
            .name(ElementName.ROOT)
            .data(pessimisticResidualLockLeaf)
            .build();
        return root;
    }

    private static Element createMultiTransactionResidualLock() {
        ScanCacheResidualLockLeaf scanCacheResidualLockLeaf = ScanCacheResidualLockLeaf.builder()
            .name(ElementName.SCAN_CACHE)
            .build();
        PessimisticResidualLockLeaf pessimisticResidualLockLeaf = PessimisticResidualLockLeaf.builder()
            .name(ElementName.PESSIMISTIC_RESIDUAL_LOCK)
            .data(scanCacheResidualLockLeaf)
            .build();
        StreamConverterLeaf streamConverterLeaf = StreamConverterLeaf.builder()
            .name(ElementName.STREAM)
            .data(pessimisticResidualLockLeaf)
            .build();
        RootLeaf root = RootLeaf.builder()
            .name(ElementName.ROOT)
            .data(streamConverterLeaf)
            .build();
        return root;
    }

    public static Element getElement(String elementName) {
        return elementMap.get(elementName);
    }
}
