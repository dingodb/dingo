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

package io.dingodb.dingokv;

import io.dingodb.dingokv.cmd.store.BaseRequest;
import io.dingodb.dingokv.cmd.store.BaseResponse;
import io.dingodb.dingokv.cmd.store.BatchDeleteRequest;
import io.dingodb.dingokv.cmd.store.BatchPutRequest;
import io.dingodb.dingokv.cmd.store.CASAllRequest;
import io.dingodb.dingokv.cmd.store.CompareAndPutRequest;
import io.dingodb.dingokv.cmd.store.ContainsKeyRequest;
import io.dingodb.dingokv.cmd.store.DeleteRangeRequest;
import io.dingodb.dingokv.cmd.store.DeleteRequest;
import io.dingodb.dingokv.cmd.store.GetAndPutRequest;
import io.dingodb.dingokv.cmd.store.GetRequest;
import io.dingodb.dingokv.cmd.store.GetSequenceRequest;
import io.dingodb.dingokv.cmd.store.KeyLockRequest;
import io.dingodb.dingokv.cmd.store.KeyUnlockRequest;
import io.dingodb.dingokv.cmd.store.MergeRequest;
import io.dingodb.dingokv.cmd.store.MultiGetRequest;
import io.dingodb.dingokv.cmd.store.NodeExecuteRequest;
import io.dingodb.dingokv.cmd.store.PutIfAbsentRequest;
import io.dingodb.dingokv.cmd.store.PutRequest;
import io.dingodb.dingokv.cmd.store.RangeSplitRequest;
import io.dingodb.dingokv.cmd.store.ResetSequenceRequest;
import io.dingodb.dingokv.cmd.store.ScanRequest;
import io.dingodb.dingokv.metadata.RegionEpoch;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface RegionKVService {
    long getRegionId();

    RegionEpoch getRegionEpoch();

    /**
     * {@link BaseRequest#PUT}
     */
    void handlePutRequest(final PutRequest request, final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#BATCH_PUT}
     */
    void handleBatchPutRequest(final BatchPutRequest request,
                               final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#PUT_IF_ABSENT}
     */
    void handlePutIfAbsentRequest(final PutIfAbsentRequest request,
                                  final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#GET_PUT}
     */
    void handleGetAndPutRequest(final GetAndPutRequest request,
                                final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#COMPARE_PUT}
     */
    void handleCompareAndPutRequest(final CompareAndPutRequest request,
                                    final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#DELETE}
     */
    void handleDeleteRequest(final DeleteRequest request,
                             final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#DELETE_RANGE}
     */
    void handleDeleteRangeRequest(final DeleteRangeRequest request,
                                  final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#BATCH_DELETE}
     */
    void handleBatchDeleteRequest(final BatchDeleteRequest request,
                                  final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#MERGE}
     */
    void handleMergeRequest(final MergeRequest request,
                            final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#GET}
     */
    void handleGetRequest(final GetRequest request, final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#MULTI_GET}
     */
    void handleMultiGetRequest(final MultiGetRequest request,
                               final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#CONTAINS_KEY}
     */
    void handleContainsKeyRequest(final ContainsKeyRequest request,
                                  final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#SCAN}
     */
    void handleScanRequest(final ScanRequest request, final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#GET_SEQUENCE}
     */
    void handleGetSequence(final GetSequenceRequest request,
                           final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#RESET_SEQUENCE}
     */
    void handleResetSequence(final ResetSequenceRequest request,
                             final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#KEY_LOCK}
     */
    void handleKeyLockRequest(final KeyLockRequest request,
                              final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#KEY_UNLOCK}
     */
    void handleKeyUnlockRequest(final KeyUnlockRequest request,
                                final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#NODE_EXECUTE}
     */
    void handleNodeExecuteRequest(final NodeExecuteRequest request,
                                  final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#RANGE_SPLIT}
     */
    void handleRangeSplitRequest(final RangeSplitRequest request,
                                 final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    /**
     * {@link BaseRequest#COMPARE_PUT_ALL}
     */
    void handleCompareAndPutAll(final CASAllRequest request,
                                final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);
}
