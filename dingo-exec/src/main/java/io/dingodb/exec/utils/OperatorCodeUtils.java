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
    public static final CommonId PART_RANGE_SCAN = new CommonId(CommonId.CommonType.OP, SOURCE, 1);
    public static final CommonId PART_RANGE_DELETE = new CommonId(CommonId.CommonType.OP, SOURCE, 2);
    public static final CommonId LIKE_SCAN = new CommonId(CommonId.CommonType.OP, SOURCE, 3);
    public static final CommonId EMPTY_SOURCE = new CommonId(CommonId.CommonType.OP, SOURCE, 4);
    public static final CommonId GET_BY_INDEX = new CommonId(CommonId.CommonType.OP, SOURCE, 5);
    public static final CommonId GET_BY_KEYS = new CommonId(CommonId.CommonType.OP, SOURCE, 6);
    public static final CommonId PART_COUNT = new CommonId(CommonId.CommonType.OP, SOURCE, 7);
    public static final CommonId PART_VECTOR = new CommonId(CommonId.CommonType.OP, SOURCE, 8);
    public static final CommonId RECEIVE = new CommonId(CommonId.CommonType.OP, SOURCE, 9);
    public static final CommonId REMOVE_PART = new CommonId(CommonId.CommonType.OP, SOURCE, 10);
    public static final CommonId VALUES = new CommonId(CommonId.CommonType.OP, SOURCE, 11);
    // txn source
    public static final CommonId TXN_PART_RANGE_SCAN = new CommonId(CommonId.CommonType.OP, SOURCE, 12);
    public static final CommonId TXN_PART_RANGE_DELETE = new CommonId(CommonId.CommonType.OP, SOURCE, 13);
    public static final CommonId TXN_LIKE_SCAN = new CommonId(CommonId.CommonType.OP, SOURCE, 14);
    public static final CommonId SCAN_CACHE = new CommonId(CommonId.CommonType.OP, SOURCE, 15);
    public static final CommonId INFO_SCHEMA_SCAN = new CommonId(CommonId.CommonType.OP, SOURCE, 16);

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
    public static final CommonId EXPORT_DATA = new CommonId(CommonId.CommonType.OP, OP, 42);

    // sink
    public static final CommonId ROOT = new CommonId(CommonId.CommonType.OP, SINK, 80);
    public static final CommonId SEND = new CommonId(CommonId.CommonType.OP, SINK, 81);
}
