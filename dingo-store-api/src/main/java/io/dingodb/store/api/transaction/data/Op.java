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

package io.dingodb.store.api.transaction.data;

public enum Op {
    NONE(0), PUT(1), DELETE(2), PUTIFABSENT(3), ROLLBACK(4), LOCK(5), CheckNotExists(6);
    private final int code;

    Op(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static boolean isNone(int code) {
        return code == NONE.getCode();
    }
    public static boolean isPut(int code) {
        return code == PUT.getCode();
    }
    public static boolean isDelete(int code) {
        return code == DELETE.getCode();
    }
    public static boolean isPutIfAbsent(int code) {
        return code == PUTIFABSENT.getCode();
    }
    public static boolean isRollBack(int code) {
        return code == ROLLBACK.getCode();
    }
    public static boolean isLock(int code) {
        return code == LOCK.getCode();
    }

    public static Op forNumber(int code) {
        switch (code) {
            case 0: return NONE;
            case 1: return PUT;
            case 2: return DELETE;
            case 3: return PUTIFABSENT;
            case 4: return ROLLBACK;
            case 5: return LOCK;
            case 6: return CheckNotExists;
            default: return null;
        }
    }
}
