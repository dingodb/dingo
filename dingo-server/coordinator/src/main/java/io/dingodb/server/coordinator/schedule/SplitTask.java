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

package io.dingodb.server.coordinator.schedule;

import io.dingodb.common.CommonId;
import io.dingodb.server.protocol.meta.Meta;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class SplitTask implements Meta {

    public enum Step {
        START,
        IGNORE,
        CREATE_NEW_PART,
        START_NEW_PART,
        UPDATE_OLD_PART,
        REASSIGN_PART,
        FINISH,
    }

    private CommonId id;
    private CommonId oldPart;
    private CommonId newPart;
    private byte[] splitKey;
    private Step step;
    private long createTime;
    private long updateTime;

}
