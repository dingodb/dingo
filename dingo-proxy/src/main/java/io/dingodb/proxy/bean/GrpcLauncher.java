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

package io.dingodb.proxy.bean;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Slf4j
@Component("grpcLauncher")
public class GrpcLauncher {

    public static final int DEFAULT_MAX_MESSAGE_SIZE = 10 * 1024 * 1024;

    private Server server;

    @Value("${server.grpc.port}")
    private Integer grpcServerPort;

    public void grpcStart(Map<String, Object> grpcServiceBeanMap) {
        try {
            ServerBuilder serverBuilder = ServerBuilder.forPort(grpcServerPort);
            for (Object bean : grpcServiceBeanMap.values()){
                serverBuilder.addService((BindableService) bean);
                log.info("{} is register in Spring Boot", bean.getClass().getSimpleName());
            }
            server = serverBuilder
                .maxInboundMessageSize(DEFAULT_MAX_MESSAGE_SIZE)
                .maxInboundMetadataSize(DEFAULT_MAX_MESSAGE_SIZE)
                .build().start();
            log.info("grpc server is started at {}", grpcServerPort);
            server.awaitTermination();
            Runtime.getRuntime().addShutdownHook(new Thread(this::grpcStop));
        } catch (IOException | InterruptedException e){
            e.printStackTrace();
        }
    }

    private void grpcStop(){
        if (server != null){
            server.shutdownNow();
        }
    }
}
