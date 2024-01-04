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

import io.dingodb.client.DingoClient;
import io.dingodb.client.vector.VectorClient;
import io.dingodb.proxy.mapper.EntityMapper;
import org.mapstruct.factory.Mappers;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClientBean {

    @Bean
    public static DingoClient dingoClient(
        @Value("${server.coordinatorExchangeSvrList}") String coordinator,
        @Value("${server.client.retry:0}") int retry
    ) {
        if (retry <= 0) {
            retry = 30;
        }
        return new DingoClient(coordinator, retry);
    }


    @Bean
    public static VectorClient vectorClient(
        @Value("${server.coordinatorExchangeSvrList}") String coordinator,
        @Value("${server.client.retry:0}") int retry
    ) {
        if (retry <= 0) {
            retry = 30;
        }
        return new VectorClient(coordinator, retry);
    }


    @Bean
    public static EntityMapper mapper() {
        return Mappers.getMapper(EntityMapper.class);
    }
}
