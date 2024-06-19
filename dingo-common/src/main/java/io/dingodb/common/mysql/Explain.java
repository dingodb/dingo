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

package io.dingodb.common.mysql;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Data
public class Explain {
    @JsonProperty("id")
    String id;
    @JsonProperty("estRows")
    double estRows;
    @JsonProperty("task")
    String task;
    @JsonProperty("accessObject")
    String accessObject;
    @JsonProperty("info")
    String info;

    List<Explain> children;

    public Explain(String id, double estRows, String task, String accessObject, String info) {
        this.id = id;
        this.estRows = estRows;
        this.accessObject = accessObject;
        this.info = info;
        this.task = task;
        this.children = new ArrayList<>();
    }

    public void output(List<Object[]> outputs, String space) {
        Object[] rows = new Object[]{space + id, estRows, task, accessObject, info};
        outputs.add(rows);
    }

    public static void loopOutput(Explain explain, List<Object[]> outputs, String space) {
        double round = Math.round(explain.getEstRows());
        Object[] rows = new Object[]{space + explain.id, round, explain.task,
            explain.accessObject, explain.info};
        outputs.add(rows);

        space = space + " ";
        for (Explain child : explain.children) {
            if (child != null) {
                loopOutput(child, outputs, space);
            } else {
                log.info("child is null:" + explain);
            }
        }
    }
}
