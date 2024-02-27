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

package io.dingodb.server.executor;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dingodb.calcite.operation.ShowLocksOperation;
import io.dingodb.common.CommonId;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.mysql.client.SessionVariableWatched;
import io.dingodb.driver.mysql.SessionVariableChangeWatcher;
import io.dingodb.exec.Services;
import io.dingodb.net.MysqlNetService;
import io.dingodb.net.MysqlNetServiceProvider;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.scheduler.SchedulerService;
import io.dingodb.server.executor.schedule.SafePointUpdateTask;
import io.dingodb.server.executor.service.ClusterService;
import io.dingodb.store.proxy.service.AutoIncrementService;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ServiceLoader;

import static io.dingodb.common.CommonId.CommonType.EXECUTOR;

@Slf4j
public class Starter {

    @Parameter(names = "--help", description = "Print usage.", help = true, order = 0)
    private boolean help;

    @Parameter(names = "--config", description = "Config file path.", order = 1, required = true)
    private String config;

    public static void main(String[] args) throws Exception {
        Starter starter = new Starter();
        JCommander commander = JCommander.newBuilder().addObject(starter).build();
        commander.parse(args);
        starter.exec(commander);
    }

    public void exec(JCommander commander) throws Exception {
        if (help) {
            commander.usage();
            return;
        }
        DingoConfiguration.parse(config);
        CommonId serverId = ClusterService.DEFAULT_INSTANCE.getServerId(DingoConfiguration.location());
        if (serverId == null) {
            serverId = new CommonId(EXECUTOR, 1, TsoService.getDefault().tso());
        }
        DingoConfiguration.instance().setServerId(serverId);
        Configuration.instance();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setRole(DingoRole.EXECUTOR);

        NetService.getDefault().listenPort(DingoConfiguration.host(), DingoConfiguration.port());
        DriverProxyServer driverProxyServer = new DriverProxyServer();
        driverProxyServer.start();
        // Register cluster heartbeat.
        ClusterService.DEFAULT_INSTANCE.register();

        Services.initControlMsgService();
        Services.initNetService();
        MysqlNetService mysqlNetService = ServiceLoader.load(MysqlNetServiceProvider.class).iterator().next().get();
        mysqlNetService.listenPort(Configuration.mysqlPort());

        SessionVariableWatched.getInstance().addObserver(new SessionVariableChangeWatcher());

        // Initialize auto increment
        AutoIncrementService.INSTANCE.resetAutoIncrement();

        SchedulerService schedulerService = SchedulerService.getDefault();
        schedulerService.init();

        // TODO Use job/task implement api.
        ApiRegistry.getDefault().register(ShowLocksOperation.Api.class, new ShowLocksOperation.Api() { });

        SafePointUpdateTask.run();
    }

}
