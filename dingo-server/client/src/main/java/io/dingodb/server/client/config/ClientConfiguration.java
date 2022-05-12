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

package io.dingodb.server.client.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import io.dingodb.common.config.DingoConfiguration;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class ClientConfiguration {

    private static final ClientConfiguration INSTANCE;

    static {
        try {
            if (DingoConfiguration.instance() == null) {
                INSTANCE = null;
            } else {
                DingoConfiguration.instance().setClient(ClientConfiguration.class);
                INSTANCE = DingoConfiguration.instance().getClient();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ClientConfiguration instance() {
        return INSTANCE;
    }

    private ClientConfiguration() {
    }

    private String coordinatorExchangeSvrList;

    public static String coordinatorExchangeSvrList() {
        return INSTANCE.coordinatorExchangeSvrList;
    }
}
