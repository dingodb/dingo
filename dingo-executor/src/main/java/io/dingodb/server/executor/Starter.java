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
import io.dingodb.calcite.executor.ShowLocksExecutor;
import io.dingodb.common.CommonId;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.meta.Tenant;
import io.dingodb.common.mysql.client.SessionVariableWatched;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.driver.mysql.SessionVariableChangeWatcher;
import io.dingodb.exec.Services;
import io.dingodb.exec.operator.InfoSchemaScanOperator;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.net.MysqlNetService;
import io.dingodb.net.MysqlNetServiceProvider;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.scheduler.SchedulerService;
import io.dingodb.server.executor.ddl.DdlContext;
import io.dingodb.server.executor.ddl.DdlServer;
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

    @Parameter(names = "--tenant", description = "Tenant id.", order = 2)
    private Long tenant;

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
        if (tenant == null) {
            String tenantStr = System.getenv().get("tenant");
            if (tenantStr != null) {
                tenant = Long.parseLong(tenantStr);
            } else {
                tenant = 0L;
            }
        }
        TenantConstant.tenant(tenant);
        CommonId serverId = ClusterService.DEFAULT_INSTANCE.getServerId(DingoConfiguration.location());
        if (serverId == null) {
            serverId = new CommonId(EXECUTOR, 1, TsoService.getDefault().tso());
        }
        DingoConfiguration.instance().setServerId(serverId);
        Configuration.instance();
        NetService.getDefault().listenPort(DingoConfiguration.host(), DingoConfiguration.port());
        DriverProxyServer driverProxyServer = new DriverProxyServer();
        driverProxyServer.start();
        // Register cluster heartbeat.
        ClusterService.DEFAULT_INSTANCE.register();
        // Register cluster heartbeat.
        log.info("Executor Configuration:{}", DingoConfiguration.instance());
        Services.initControlMsgService();
        Services.initNetService();

        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        env.setRole(DingoRole.EXECUTOR);
        SchedulerService schedulerService = SchedulerService.getDefault();
        checkContinue();
        Object tenantObj = Optional.mapOrGet(InfoSchemaService.root(), __ -> __.getTenant(tenant), () -> null);
        if (tenantObj == null) {
            log.error("The tenant: {} was not found.", tenant);
            System.exit(0);
        }
        if (((Tenant) tenantObj).isDelete()) {
            log.error("The tenant: {} has been deleted and is unavailable", tenant);
            System.exit(0);
        }
        schedulerService.init();

        MysqlNetService mysqlNetService = ServiceLoader.load(MysqlNetServiceProvider.class).iterator().next().get();
        mysqlNetService.listenPort(Configuration.mysqlPort());

        SessionVariableWatched.getInstance().addObserver(new SessionVariableChangeWatcher());

        // Initialize auto increment
        AutoIncrementService.INSTANCE.resetAutoIncrement();

        ApiRegistry.getDefault().register(ShowLocksExecutor.Api.class, new ShowLocksExecutor.Api() { });
        ApiRegistry.getDefault().register(InfoSchemaScanOperator.Api.class, new InfoSchemaScanOperator.Api() { });

        SafePointUpdateTask.run();

        DdlServer.startDispatchLoop();
    }

    public static void checkContinue() {
        boolean ready = false;
        while (!ready) {
            if (DdlContext.getPrepare()) {
                ready = true;
            }
            Utils.sleep(500);
        }
    }

}
