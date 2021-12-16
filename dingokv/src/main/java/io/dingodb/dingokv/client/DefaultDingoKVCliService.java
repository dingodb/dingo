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

package io.dingodb.dingokv.client;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.CliServiceImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.CliClientService;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.AbstractClientService;
import com.alipay.sofa.jraft.util.Requires;
import io.dingodb.dingokv.cmd.store.BaseResponse;
import io.dingodb.dingokv.cmd.store.RangeSplitRequest;
import io.dingodb.dingokv.util.StackTraceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class DefaultDingoKVCliService implements DingoKVCliService {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDingoKVCliService.class);

    private RpcClient rpcClient;
    private CliService cliService;
    private CliOptions opts;

    private boolean started;

    @Override
    public boolean init(final CliOptions opts) {
        if (this.started) {
            LOG.info("[DefaultDingoKVRpcService] already started.");
            return true;
        }
        initCli(opts);
        LOG.info("[DefaultDingoKVCliService] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public void shutdown() {
        if (this.cliService != null) {
            this.cliService.shutdown();
        }
        this.started = false;
        LOG.info("[DefaultDingoKVCliService] shutdown successfully.");
    }

    @Override
    public Status rangeSplit(final long regionId, final long newRegionId, final String groupId, final Configuration conf) {
        final PeerId leaderId = new PeerId();
        final Status st = this.cliService.getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            throw new IllegalStateException(st.getErrorMsg());
        }
        final RangeSplitRequest request = new RangeSplitRequest();
        request.setRegionId(regionId);
        request.setNewRegionId(newRegionId);
        try {
            final BaseResponse<?> response = (BaseResponse<?>) this.rpcClient.invokeSync(leaderId.getEndpoint(),
                request, this.opts.getTimeoutMs());
            if (response.isSuccess()) {
                return Status.OK();
            }
            return new Status(-1, "Fail to range split on region %d, error: %s", regionId, response);
        } catch (final Exception e) {
            LOG.error("Fail to range split on exception: {}.", StackTraceUtil.stackTrace(e));
            return new Status(-1, "fail to range split on region %d", regionId);
        }
    }

    private void initCli(CliOptions cliOpts) {
        if (cliOpts == null) {
            cliOpts = new CliOptions();
            cliOpts.setTimeoutMs(5000);
            cliOpts.setMaxRetry(3);
        }
        this.opts = cliOpts;
        this.cliService = RaftServiceFactory.createAndInitCliService(cliOpts);
        final CliClientService cliClientService = ((CliServiceImpl) this.cliService).getCliClientService();
        Requires.requireNonNull(cliClientService, "cliClientService");
        this.rpcClient = ((AbstractClientService) cliClientService).getRpcClient();
    }
}
