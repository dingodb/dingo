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

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.impl.OptimisticTransaction;
import io.dingodb.exec.transaction.visitor.DingoTransactionRenderJob;
import io.dingodb.net.Channel;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Slf4j
public class DingoTransactionRenderJobTest {
    private Job job;
    private Location currentLocation;
    private ITransaction transaction;

    @BeforeEach
    public void setUp() {
        long start_ts = TsoService.getDefault().tso();
        CommonId commonId = new CommonId(CommonId.CommonType.TRANSACTION, TsoService.getDefault().tso(), start_ts);
        long jobSeq = TsoService.getDefault().tso();
        job = JobManagerImpl.INSTANCE.createJob(start_ts, jobSeq, commonId, null);
        currentLocation = new Location("localhost", 10000);
//        transaction = new OptimisticTransaction(commonId);
        transaction = mock(OptimisticTransaction.class);
        Channel channel3 = mock(Channel.class);
        Channel channel4 = mock(Channel.class);

//        NetServiceProvider netServiceProvider = NetServiceProvider.getDefault();
//        Channel channel1 = Services.openNewChannel("172.20.3.30", 10000);
//        Channel channel2 = Services.openNewChannel("172.20.3.31", 10000);
//        transaction.registerChannel(new CommonId(CommonId.CommonType.OP, 0, 0), channel3);
//        transaction.registerChannel(new CommonId(CommonId.CommonType.OP, 0, 1), channel4);
    }

    @AfterEach
    public void tearDown() {
        job = null;
        currentLocation = null;
        transaction = null;
    }

    @Test
    public void testRenderPreWriteJob() {
        DingoTransactionRenderJob.renderPreWriteJob(job, currentLocation, transaction, true);
        System.out.println(job.toString());
        assertThat(job.getTasks().size() == 1);
    }

    @Test
    public void testRenderCommitJob() {
        DingoTransactionRenderJob.renderCommitJob(job, currentLocation, transaction, true);
        System.out.println(job.toString());
        assertThat(job.getTasks().size() == 1);
    }

    @Test
    public void testRenderRollBackJob() {
        DingoTransactionRenderJob.renderRollBackJob(job, currentLocation, transaction, true);
        System.out.println(job.toString());
        assertThat(job.getTasks().size() == 1);
    }


}
