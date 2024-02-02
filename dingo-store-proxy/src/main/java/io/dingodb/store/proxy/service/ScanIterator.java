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
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.common.RangeWithOptions;
import io.dingodb.sdk.service.entity.store.Coprocessor;
import io.dingodb.sdk.service.entity.store.KvScanBeginRequest;
import io.dingodb.sdk.service.entity.store.KvScanBeginResponse;
import io.dingodb.sdk.service.entity.store.KvScanContinueRequest;
import io.dingodb.sdk.service.entity.store.KvScanContinueResponse;
import io.dingodb.sdk.service.entity.store.KvScanReleaseRequest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.dingodb.sdk.service.entity.error.Errno.OK;

@Slf4j
public class ScanIterator implements Iterator<KeyValue>, AutoCloseable {

    private final CommonId regionId;
    private final ChannelProvider channelProvider;
    private long requestTs;
    private StoreService storeService;
    private byte[] scanId;
    private Coprocessor coprocessor;
    private RangeWithOptions range;

    private final int retryTimes;

    private Iterator<KeyValue> delegateIterator = Collections.emptyIterator();
    private boolean release = false;

    public ScanIterator(
        long requestTs,
        CommonId regionId,
        ChannelProvider channelProvider,
        RangeWithOptions range,
        Coprocessor coprocessor,
        int retryTimes
    ) {
        this.regionId = regionId;
        this.range = range;
        this.retryTimes = retryTimes;
        this.coprocessor = coprocessor;
        this.requestTs = requestTs;
        this.scanId = scanBegin(channelProvider);
        if (scanId == null || scanId.length == 0) {
            release = true;
        }
        this.channelProvider = channelProvider;
    }

    public byte[] scanBegin(ChannelProvider channelProvider) {
        int retry = retryTimes;
        Optional.ofNullable(coprocessor).map($ -> $.getOriginalSchema()).ifPresent($ -> $.setCommonId(regionId.domain));
        Optional.ofNullable(coprocessor).map($ -> $.getResultSchema()).ifPresent($ -> $.setCommonId(regionId.domain));
        while (retry-- > 0) {
            Channel channel = channelProvider.channel();
            try {
                KvScanBeginRequest request = KvScanBeginRequest.builder()
                    .coprocessor(coprocessor)
                    .range(range)
                    .build();
                channelProvider.before(request);
                KvScanBeginResponse res = RpcCaller.call(
                    StoreServiceDescriptors.kvScanBegin,
                    request,
                    CallOptions.DEFAULT,
                    channel,
                    requestTs,
                    StoreServiceDescriptors.kvScanBeginHandlers
                );
                channelProvider.after(res);
                if (res != null && (res.getError() == null || res.getError().getErrcode() == OK)) {
                    this.storeService = createStoreService(channel);
                    return res.getScanId();
                }
                if (log.isDebugEnabled()) {

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
        if (delegateIterator.hasNext()) {
            return;
        }
        KvScanContinueRequest request = KvScanContinueRequest.builder()
            .scanId(scanId)
            .maxFetchCnt(1000)
            .build();
        channelProvider.before(request);
        KvScanContinueResponse res = storeService.kvScanContinue(request);
        channelProvider.after(res);
        delegateIterator = Optional.mapOrGet(res.getKvs(), List::iterator, Collections::emptyIterator);
        if (!delegateIterator.hasNext()) {
            release = true;
            CompletableFuture.runAsync(this::scanRelease);
        }
    }


    public void scanRelease() {
        KvScanReleaseRequest request = KvScanReleaseRequest.builder().scanId(scanId).build();
        channelProvider.before(request);
        channelProvider.after(storeService.kvScanRelease(request));
    }

    @Override
    public synchronized void close() {
        if (release) {
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
        if (release) {
            return false;
        }
        if (delegateIterator.hasNext()) {
            return true;
        }
        scanContinue();
        return delegateIterator.hasNext();
    }

    @Override
    public KeyValue next() {
        if (release) {
            throw new NoSuchElementException();
        }
        return delegateIterator.next();
    }

}
