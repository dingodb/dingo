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

package io.dingodb.common.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import io.dingodb.common.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class SecurityConfiguration {
    private CipherConfiguration cipher;
    private boolean verify = true;
    private boolean auth = true;

    public static boolean isAuth() {
        return Optional.ofNullable(DingoConfiguration.instance())
            .map(DingoConfiguration::getSecurity)
            .map(s -> s.auth)
            .orElse(true);
    }

    public static boolean isVerify() {
        return Optional.ofNullable(DingoConfiguration.instance())
            .map(DingoConfiguration::getSecurity)
            .map(s -> s.verify)
            .orElse(true);
    }

    public static CipherConfiguration cipher() {
        return Optional.ofNullable(DingoConfiguration.instance())
            .map(DingoConfiguration::getSecurity)
            .map(s -> s.cipher)
            .orNull();
    }

}
