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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class SecurityCipher {

    public static SecurityCipher securityCipher;

    @Value("${security.cipher.keyPath}")
    public String keyPath;

    @Value("${security.cipher.keyPass}")
    public String keyPass;

    @Value("${security.cipher.storePass}")
    public String storePass;

    @Value("${security.cipher.alias}")
    public String alias;

    @Value("${security.cipher.issuer}")
    public String issuer;

    @PostConstruct
    public void init() {
        securityCipher = this;
    }
}
