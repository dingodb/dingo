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

package io.dingodb.common.exception;

import lombok.Getter;
import lombok.Setter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DingoSqlException extends RuntimeException {
    public static final String NULL_MESSAGE = "No messages";
    public static final int UNKNOWN_ERROR_CODE = 9001;
    public static final int TEST_ERROR_CODE = 9002;

    public static final String CUSTOM_ERROR_STATE = "45000";

    private static final long serialVersionUID = -952945364943472362L;
    private static final Pattern pattern = Pattern.compile("Error (\\d+)\\s*\\((\\w+)\\):\\s*(.*)");

    @Getter
    @Setter
    private int sqlCode;
    @Getter
    @Setter
    private String sqlState;
    @Getter
    @Setter
    private String message;

    public DingoSqlException() {
        this(null);
    }

    public DingoSqlException(String message) {
        this(message, null);
    }

    public DingoSqlException(String message, Throwable cause) {
        super(message, cause);
        if (message != null) {
            Matcher matcher = pattern.matcher(message);
            boolean result = matcher.find();
            if (result) {
                this.sqlCode = Integer.parseInt(matcher.group(1));
                this.sqlState = matcher.group(2);
                StringBuffer sb = new StringBuffer();
                matcher.appendReplacement(sb, "$3");
                matcher.appendTail(sb);
                this.message = sb.toString();
                return;
            }
            this.message = message;
        } else {
            this.message = NULL_MESSAGE;
        }
        this.sqlCode = UNKNOWN_ERROR_CODE;
        this.sqlState = CUSTOM_ERROR_STATE;
    }

    public DingoSqlException(String message, int sqlCode, String sqlState) {
        super(message, null);
        this.message = (message != null ? message : NULL_MESSAGE);
        this.sqlCode = sqlCode;
        this.sqlState = (sqlState != null ? sqlState : Integer.toString(sqlCode));
    }
}
