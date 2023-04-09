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

package io.dingodb.common.mysql.constant;

public class ColumnStatus {
    public static final int COLUMN_NULLABLE = 0X0001;

    public static final int COLUMN_PRIMARY = 0X0002;

    public static final int COLUMN_UNIQUE = 0X0004;

    public static final int COLUMN_MULTIPLE = 0X0008;

    public static final int COLUMN_BLOB = 0X0010;

    public static final int COLUMN_UNSIGNED = 0X0020;

    public static final int COLUMN_ZEROFILL = 0X0040;

    public static final int COLUMN_ENUM = 0X0100;

    public static final int COLUMN_AUTOINCREMENT = 0X0200;

    public static final int COLUMN_TIMESTAMP = 0X0400;

    public static final int COLUMN_SET = 0X0800;

    public static final int allEmpty = 0x8000;
}
