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

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDingoConfiguration {

    @ToString
    @EqualsAndHashCode
    @NoArgsConstructor
    @AllArgsConstructor
    public static class A {
        int id;
        String name;
        B b;
        Integer ccc;
    }

    @ToString
    @EqualsAndHashCode
    @NoArgsConstructor
    @AllArgsConstructor
    public static class B {
        int id;
        String name;
        C c;
    }

    @ToString
    @EqualsAndHashCode
    @NoArgsConstructor
    @AllArgsConstructor
    public static class C {
        int id;
        String ccc;
    }

    @ToString
    @EqualsAndHashCode
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AList {
        int id;
        C c;
        List<Integer> ss;
    }

    @Test
    @SneakyThrows
    public void testParse() {
        DingoConfiguration.parse(TestDingoConfiguration.class.getResource("/config.yaml").getPath());
        assertThat(DingoConfiguration.instance().getConfig("configA", A.class))
            .isEqualTo(new A(1, "nameA", new B(2, "nameB", new C(3, "abc")), null));
        assertThat(DingoConfiguration.instance().getConfig("listA", AList.class))
            .isEqualTo(new AList(1234, new C(3, null), Arrays.asList(1, 2, 3)));
        assertThat(DingoConfiguration.instance().find("ccc", String.class))
            .isEqualTo("abc");
        assertThat(DingoConfiguration.instance().find("ss", List.class))
            .isEqualTo(Arrays.asList(1, 2, 3));
    }

}
