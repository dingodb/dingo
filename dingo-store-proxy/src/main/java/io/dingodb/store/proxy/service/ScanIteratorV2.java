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

package io.dingodb.store.proxy.service;

import io.dingodb.common.CommonId;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.ChannelProvider;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.caller.RpcCaller;
import io.dingodb.sdk.service.desc.store.StoreServiceDescriptors;
import io.dingodb.sdk.service.entity.common.CoprocessorV2;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.common.RangeWithOptions;
import io.dingodb.sdk.service.entity.store.KvScanBeginRequestV2;
import io.dingodb.sdk.service.entity.store.KvScanBeginResponseV2;
import io.dingodb.sdk.service.entity.store.KvScanContinueRequestV2;
import io.dingodb.sdk.service.entity.store.KvScanContinueResponseV2;
import io.dingodb.sdk.service.entity.store.KvScanReleaseRequestV2;
import io.dingodb.sdk.service.entity.store.KvScanReleaseResponseV2;
import io.grpc.CallOptions;
import io.grpc.Channel;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.dingodb.sdk.service.entity.error.Errno.OK;

@Slf4j
public class ScanIteratorV2 implements Iterator<KeyValue>, AutoCloseable {
    private final CommonId regionId;
    private final ChannelProvider channelProvider;
    private StoreService storeService;
    private final long requestTs;
    private final long scanId;
    private final CoprocessorV2 coprocessor;
    private final RangeWithOptions range;

    private final int retryTimes;

    private Iterator<KeyValue> delegateIterator = Collections.emptyIterator();
    private boolean hasMore;

    public ScanIteratorV2(
        long requestTs,
        CommonId regionId,
        ChannelProvider channelProvider,
        RangeWithOptions range,
        CoprocessorV2 coprocessor,
        int retryTimes
    ) {
        this.regionId = regionId;
        this.range = range;
        this.retryTimes = retryTimes;
        this.coprocessor = coprocessor;
        this.requestTs = requestTs;
        this.scanId = scanBegin(requestTs, channelProvider);
        this.channelProvider = channelProvider;
        this.hasMore = (scanId != 0);
    }

    public long scanBegin(long requestTs, ChannelProvider channelProvider) {
        int retry = retryTimes;
        Optional.ofNullable(coprocessor).map(CoprocessorV2::getOriginalSchema).ifPresent($ -> $.setCommonId(regionId.domain));
        Optional.ofNullable(coprocessor).map(CoprocessorV2::getResultSchema).ifPresent($ -> $.setCommonId(regionId.domain));
        while (retry-- > 0) {
            Channel channel = channelProvider.channel();
            try {
                long scanId = TsoService.INSTANCE.tso();
                KvScanBeginRequestV2 request = KvScanBeginRequestV2.builder()
                    .scanId(scanId)
                    .coprocessor(coprocessor)
                    .range(range)
                    .build();
                channelProvider.before(request);
                if (log.isDebugEnabled()) {
                    log.debug("Emit ScanBeginV2: scanId = {}, request ts = {}", scanId, requestTs);
                }
                KvScanBeginResponseV2 res = RpcCaller.call(
                    StoreServiceDescriptors.kvScanBeginV2,
                    request,
                    CallOptions.DEFAULT,
                    channel,
                    requestTs,
                    StoreServiceDescriptors.kvScanBeginV2Handlers
                );
                channelProvider.after(res);
                if (res != null) {
                    if (res.getError() == null || res.getError().getErrcode() == OK) {
                        this.storeService = createStoreService(channel);
                        return res.getScanId();
                    }
                }
            } catch (Exception ignored) {
            }
            channelProvider.refresh(channel, requestTs);
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        throw new RuntimeException("Scan begin retry >= " + retryTimes);
    }

    @NonNull
    private static StoreService createStoreService(Channel channel) {
        return (StoreService) Proxy.newProxyInstance(
            StoreService.class.getClassLoader(),
            new Class[]{StoreService.class},
            new RpcCaller<>(channel, CallOptions.DEFAULT, StoreService.class)
        );
    }

    public synchronized void scanContinue() {
        if (log.isDebugEnabled()) {
            log.debug("Emit ScanContinueV2: scanId = {}", scanId);
        }
        KvScanContinueRequestV2 request = KvScanContinueRequestV2.builder()
            .scanId(scanId)
            .maxFetchCnt(1000)
            .build();
        channelProvider.before(request);
        KvScanContinueResponseV2 res = storeService.kvScanContinueV2(requestTs, request);
        channelProvider.after(res);
        if (res != null && res.getError() != null && res.getError().getErrcode() != OK) {
            hasMore = false;
            scanRelease();
            throw new RuntimeException(res.getError().getErrmsg());
        }
        delegateIterator = Optional.mapOrGet(res.getKvs(), List::iterator, Collections::emptyIterator);
        if (!res.isHasMore()) {
            hasMore = false;
            scanRelease();
        }
    }

    public void scanRelease() {
        if (log.isDebugEnabled()) {
            log.debug("Emit ScanReleaseV2: scanId = {}", scanId);
        }
        KvScanReleaseRequestV2 request = KvScanReleaseRequestV2.builder().scanId(scanId).build();
        channelProvider.before(request);
        KvScanReleaseResponseV2 response = storeService.kvScanReleaseV2(requestTs, request);
        channelProvider.after(response);
    }

    @Override
    public synchronized void close() {
        if (!hasMore) {
            return;
        }
        scanRelease();
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    @Override
    public boolean hasNext() {
        while (hasMore && !delegateIterator.hasNext()) {
            scanContinue();
        }
        return delegateIterator.hasNext();
    }

    @Override
    public KeyValue next() {
        return delegateIterator.next();
    }
}
