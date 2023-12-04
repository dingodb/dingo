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

package io.dingodb.proxy;

import io.dingodb.proxy.annotation.GrpcService;
import io.dingodb.proxy.bean.GrpcLauncher;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import springfox.documentation.oas.annotations.EnableOpenApi;

import java.util.Map;

@Configurable
@EnableOpenApi
@SpringBootApplication
public class DingoApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(DingoApplication.class, args);
        Map<String, Object> grpcServiceMap = context.getBeansWithAnnotation(GrpcService.class);
        GrpcLauncher launcher = context.getBean("grpcLauncher", GrpcLauncher.class);
        launcher.grpcStart(grpcServiceMap);
    }
}
