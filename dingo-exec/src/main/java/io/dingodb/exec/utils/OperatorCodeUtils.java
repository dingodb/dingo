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

package io.dingodb.exec.utils;

import io.dingodb.common.CommonId;

public final class OperatorCodeUtils {

    public static final Long SOURCE = 0L;
    public static final Long OP = 1L;
    public static final Long SINK = 2L;

    // source
    public static final CommonId CALC_DISTRIBUTION = new CommonId(CommonId.CommonType.OP, SOURCE, 1);
    public static final CommonId GET_DISTRIBUTION = new CommonId(CommonId.CommonType.OP, SOURCE, 2);
    public static final CommonId LIKE_SCAN = new CommonId(CommonId.CommonType.OP, SOURCE, 3);
    public static final CommonId EMPTY_SOURCE = new CommonId(CommonId.CommonType.OP, SOURCE, 4);
    public static final CommonId PART_COUNT = new CommonId(CommonId.CommonType.OP, SOURCE, 5);
    public static final CommonId PART_VECTOR = new CommonId(CommonId.CommonType.OP, SOURCE, 6);
    public static final CommonId RECEIVE = new CommonId(CommonId.CommonType.OP, SOURCE, 7);
    public static final CommonId REMOVE_PART = new CommonId(CommonId.CommonType.OP, SOURCE, 8);
    public static final CommonId VALUES = new CommonId(CommonId.CommonType.OP, SOURCE, 9);
    // txn source
    public static final CommonId TXN_LIKE_SCAN = new CommonId(CommonId.CommonType.OP, SOURCE, 11);
    public static final CommonId SCAN_CACHE = new CommonId(CommonId.CommonType.OP, SOURCE, 12);
    public static final CommonId INFO_SCHEMA_SCAN = new CommonId(CommonId.CommonType.OP, SOURCE, 13);
    public static final CommonId TXN_PART_VECTOR = new CommonId(CommonId.CommonType.OP, SOURCE, 14);
    public static final CommonId CALC_DISTRIBUTION_1 = new CommonId(CommonId.CommonType.OP, SOURCE, 15);

    // op
    public static final CommonId PROJECT = new CommonId(CommonId.CommonType.OP, OP, 20);
    public static final CommonId FILTER = new CommonId(CommonId.CommonType.OP, OP, 21);
    public static final CommonId AGGREGATE = new CommonId(CommonId.CommonType.OP, OP, 22);
    public static final CommonId COALESCE = new CommonId(CommonId.CommonType.OP, OP, 23);
    public static final CommonId HASH = new CommonId(CommonId.CommonType.OP, OP, 24);
    public static final CommonId HASH_JOIN = new CommonId(CommonId.CommonType.OP, OP, 25);
    public static final CommonId INDEX_MERGE = new CommonId(CommonId.CommonType.OP, OP, 26);
    public static final CommonId PART_DELETE = new CommonId(CommonId.CommonType.OP, OP, 27);
    public static final CommonId PART_INSERT = new CommonId(CommonId.CommonType.OP, OP, 28);
    public static final CommonId PART_UPDATE = new CommonId(CommonId.CommonType.OP, OP, 29);
    public static final CommonId PARTITION = new CommonId(CommonId.CommonType.OP, OP, 30);
    public static final CommonId REDUCE = new CommonId(CommonId.CommonType.OP, OP, 31);
    public static final CommonId SORT = new CommonId(CommonId.CommonType.OP, OP, 32);
    public static final CommonId SUM_UP = new CommonId(CommonId.CommonType.OP, OP, 33);
    public static final CommonId VECTOR_PARTITION = new CommonId(CommonId.CommonType.OP, OP, 34);
    public static final CommonId VECTOR_POINT_DISTANCE = new CommonId(CommonId.CommonType.OP, OP, 35);
    // txn op
    public static final CommonId TXN_PART_INSERT = new CommonId(CommonId.CommonType.OP, OP, 36);
    public static final CommonId TXN_PART_DELETE = new CommonId(CommonId.CommonType.OP, OP, 37);
    public static final CommonId TXN_PART_UPDATE = new CommonId(CommonId.CommonType.OP, OP, 38);
    public static final CommonId COMMIT = new CommonId(CommonId.CommonType.OP, OP, 39);
    public static final CommonId PRE_WRITE = new CommonId(CommonId.CommonType.OP, OP, 40);
    public static final CommonId ROLL_BACK = new CommonId(CommonId.CommonType.OP, OP, 41);
    public static final CommonId COMPARE_AND_SET = new CommonId(CommonId.CommonType.OP, OP, 42);
    public static final CommonId PESSIMISTIC_LOCK_DELETE = new CommonId(CommonId.CommonType.OP, OP, 43);
    public static final CommonId PESSIMISTIC_LOCK_INSERT = new CommonId(CommonId.CommonType.OP, OP, 44);
    public static final CommonId PESSIMISTIC_LOCK_UPDATE = new CommonId(CommonId.CommonType.OP, OP, 45);
    public static final CommonId PESSIMISTIC_ROLL_BACK = new CommonId(CommonId.CommonType.OP, OP, 46);
    public static final CommonId EXPORT_DATA = new CommonId(CommonId.CommonType.OP, OP, 47);

    public static final CommonId DISTRIBUTE = new CommonId(CommonId.CommonType.OP, OP, 48);
    public static final CommonId PART_RANGE_SCAN = new CommonId(CommonId.CommonType.OP, OP, 49);
    public static final CommonId PART_RANGE_DELETE = new CommonId(CommonId.CommonType.OP, OP, 50);
    public static final CommonId TXN_PART_RANGE_SCAN = new CommonId(CommonId.CommonType.OP, OP, 51);
    public static final CommonId GET_BY_INDEX = new CommonId(CommonId.CommonType.OP, OP, 52);
    public static final CommonId GET_BY_KEYS = new CommonId(CommonId.CommonType.OP, OP, 53);
    public static final CommonId TXN_PART_RANGE_DELETE = new CommonId(CommonId.CommonType.OP, OP, 54);
    public static final CommonId TXN_CLEAN_CACHE = new CommonId(CommonId.CommonType.OP, OP, 55);
    public static final CommonId COPY = new CommonId(CommonId.CommonType.OP, OP, 56);

    public static final CommonId PIPE_OP = new CommonId(CommonId.CommonType.OP, OP, 60);
    public static final CommonId CACHE_OP = new CommonId(CommonId.CommonType.OP, OP, 61);
    public static final CommonId SCAN_WITH_NO_OP = new CommonId(CommonId.CommonType.OP, OP, 62);
    public static final CommonId SCAN_WITH_PIPE_OP = new CommonId(CommonId.CommonType.OP, OP, 63);
    public static final CommonId SCAN_WITH_CACHE_OP = new CommonId(CommonId.CommonType.OP, OP, 64);
    public static final CommonId TXN_GET_BY_KEYS = new CommonId(CommonId.CommonType.OP, OP, 65);
    public static final CommonId TXN_SCAN_WITH_NO_OP = new CommonId(CommonId.CommonType.OP, OP, 66);
    public static final CommonId TXN_SCAN_WITH_PIPE_OP = new CommonId(CommonId.CommonType.OP, OP, 67);
    public static final CommonId TXN_SCAN_WITH_CACHE_OP = new CommonId(CommonId.CommonType.OP, OP, 68);
    public static final CommonId TXN_GET_BY_INDEX = new CommonId(CommonId.CommonType.OP, OP, 69);
    public static final CommonId PESSIMISTIC_LOCK = new CommonId(CommonId.CommonType.OP, OP, 70);
    public static final CommonId PESSIMISTIC_RESIDUAL_LOCK = new CommonId(CommonId.CommonType.OP, OP, 71);
    public static final CommonId REDUCE_REL_OP = new CommonId(CommonId.CommonType.OP, OP, 72);

    // sink
    public static final CommonId ROOT = new CommonId(CommonId.CommonType.OP, SINK, 80);
    public static final CommonId SEND = new CommonId(CommonId.CommonType.OP, SINK, 81);

    private OperatorCodeUtils() {
    }
}
