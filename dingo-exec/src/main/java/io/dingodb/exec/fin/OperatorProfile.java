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

package io.dingodb.exec.fin;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import lombok.Data;

import java.text.SimpleDateFormat;
import java.util.Date;

@Data
public class OperatorProfile {
    @JsonProperty("id")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    CommonId operatorId;
    @JsonProperty("start")
    long startTimeStamp;
    @JsonProperty("end")
    long endTimeStamp;
    @JsonProperty("count")
    long processedTupleCount;

    @JsonProperty("autoIncId")
    long autoIncId;

    public String detail() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
        return "Operator " + operatorId + ":"
            + " Start: " + dateFormat.format(new Date(startTimeStamp))
            + " End: " + dateFormat.format(new Date(endTimeStamp))
            + " Duration: " + (endTimeStamp - startTimeStamp) + "ms"
            + " Count: " + processedTupleCount;
    }
}
