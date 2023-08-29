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

package io.dingodb.test.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCsvUtils {
    @Test
    public void testReadCsvLine() throws JsonProcessingException {
        DingoType schema = DingoTypeFactory.tuple("INT", "STRING", "DOUBLE");
        List<Object[]> values = CsvUtils.readCsv(schema, "1, Alice, 1.0\n2, Betty, 2.0");
        Object[] tuple = values.get(0);
        assertThat(tuple[0]).isEqualTo(1);
        assertThat(tuple[1]).isEqualTo("Alice");
        assertThat(tuple[2]).isEqualTo(1.0);
        tuple = values.get(1);
        assertThat(tuple[0]).isEqualTo(2);
        assertThat(tuple[1]).isEqualTo("Betty");
        assertThat(tuple[2]).isEqualTo(2.0);
    }
}
