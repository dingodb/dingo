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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static io.dingodb.expr.json.runtime.Parser.YAML;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDingoConfiguration {

    private static DingoConfiguration configuration = DingoConfiguration.instance();

    @BeforeAll
    public static void beforeAll() throws IOException {
        YAML.parse(TestDingoConfiguration.class.getResourceAsStream("/config.yaml"), Map.class)
            .forEach((k, v) -> configuration.set(k.toString(), v));
    }

    @Test
    public void test01() {
        assertThat(configuration.getString("a.b2.c"))
            .isEqualTo("c");
    }

    @Test
    public void test02() {
        assertThat(configuration.getInt("a.b1"))
            .isEqualTo(1234);
    }

    @Test
    public void test03() throws Exception {
        ConfigBeanA configBeanA = new ConfigBeanA(1L, "nameA", new ConfigBeanB(2L, "nameB", new ConfigBeanC(3, null)));
        assertThat(configuration.getAndConvert("configA", ConfigBeanA.class))
            .isEqualTo(configBeanA);
    }

    @Test
    public void test04() throws Exception {
        assertThat(configuration.getList("a.b3"))
            .isEqualTo(Arrays.asList(1, 2, 3));
    }

}
