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

package io.dingodb.dingokv.metadata;

import com.alipay.sofa.jraft.util.Copiable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class TimeInterval implements Copiable<TimeInterval>, Serializable {

    private static final long serialVersionUID = 1L;

    private long              startTimestamp;
    private long              endTimestamp;

    public TimeInterval() {
    }

    @Override
    public TimeInterval copy() {
        return new TimeInterval(this.startTimestamp, this.endTimestamp);
    }

}
