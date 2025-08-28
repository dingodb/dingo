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

import java.util.Base64;
import java.util.Objects;

public class DingoErr {
    public int errorCode;
    public String state;
    public String errorMsg;
    public boolean encodeError;
    public String warning;

    public DingoErr() {

    }

    public DingoErr(String warning) {
        this.warning = warning;
    }

    public DingoErr(int errorCode, String state, String errorMsg) {
        this.errorCode = errorCode;
        this.state = state;
        this.errorMsg = Objects.requireNonNullElse(errorMsg, ".");
    }

    public void fillErrorByArgs(Object... param) {
        int paramCnt = param.length;
        if (paramCnt == 1) {
            errorMsg = String.format(errorMsg, param[0]);
        } else if (paramCnt == 2) {
            errorMsg = String.format(errorMsg, param[0], param[1]);
        } else if (paramCnt == 3) {
            errorMsg = String.format(errorMsg, param[0], param[1], param[2]);
        }
    }

    public void encodeError() {
        if (this.errorMsg != null && !this.encodeError) {
            this.errorMsg = Base64.getEncoder().encodeToString(errorMsg.getBytes());
            this.encodeError = true;
        }
    }

    public void decodeError() {
        if (this.errorMsg != null) {
            this.errorMsg = new String(Base64.getDecoder().decode(this.errorMsg));
            this.encodeError = false;
        }
    }

    @Override
    public String toString() {
        return "DingoErr{"
            + "errorCode=" + errorCode
            + ", state='" + state + '\''
            + ", errorMsg='" + errorMsg + '\''
            + '}';
    }
}
