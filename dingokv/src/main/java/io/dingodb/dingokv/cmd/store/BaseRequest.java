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

package io.dingodb.dingokv.cmd.store;

import io.dingodb.dingokv.metadata.RegionEpoch;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Getter
@Setter
@ToString
public abstract class BaseRequest implements Serializable {
    private static final long serialVersionUID = -6576381361684687237L;

    public static final byte  PUT = 0x01;
    public static final byte  BATCH_PUT = 0x02;
    public static final byte  PUT_IF_ABSENT = 0x03;
    public static final byte  GET_PUT = 0x04;
    public static final byte  DELETE = 0x05;
    public static final byte  DELETE_RANGE = 0x06;
    public static final byte  MERGE = 0x07;
    public static final byte  GET = 0x08;
    public static final byte  MULTI_GET = 0x09;
    public static final byte  SCAN = 0x0a;
    public static final byte  GET_SEQUENCE = 0x0b;
    public static final byte  RESET_SEQUENCE = 0x0c;
    public static final byte  KEY_LOCK = 0x0d;
    public static final byte  KEY_UNLOCK = 0x0e;
    public static final byte  NODE_EXECUTE = 0x0f;
    public static final byte  RANGE_SPLIT = 0x10;
    public static final byte  COMPARE_PUT = 0x11;
    public static final byte  BATCH_DELETE = 0x12;
    public static final byte  CONTAINS_KEY = 0x13;
    public static final byte  COMPARE_PUT_ALL = 0x14;

    private long regionId;
    private RegionEpoch regionEpoch;

    public abstract byte magic();
}
