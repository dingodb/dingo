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

package io.dingodb.raft;

import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.util.Copiable;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class Status implements Copiable<Status> {
    /**
     * Status internal state.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 11:17:51 AM
     */
    private static class State {
        /** error code */
        int code;
        /** error msg*/
        String msg;

        State(int code, String msg) {
            super();
            this.code = code;
            this.msg = msg;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + this.code;
            result = prime * result + (this.msg == null ? 0 : this.msg.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            State other = (State) obj;
            if (this.code != other.code) {
                return false;
            }
            if (this.msg == null) {
                return other.msg == null;
            } else {
                return this.msg.equals(other.msg);
            }
        }
    }

    private State state;

    public Status() {
        this.state = null;
    }

    /**
     * Creates a OK status instance.
     */
    public static Status OK() {
        return new Status();
    }

    public Status(Status s) {
        if (s.state != null) {
            this.state = new State(s.state.code, s.state.msg);
        } else {
            this.state = null;
        }
    }

    public Status(RaftError raftError, String fmt, Object... args) {
        this.state = new State(raftError.getNumber(), String.format(fmt, args));
    }

    public Status(int code, String fmt, Object... args) {
        this.state = new State(code, String.format(fmt, args));
    }

    public Status(int code, String errorMsg) {
        this.state = new State(code, errorMsg);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.state == null ? 0 : this.state.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Status other = (Status) obj;
        if (this.state == null) {
            return other.state == null;
        } else {
            return this.state.equals(other.state);
        }
    }

    /**
     * Reset status to be OK state.
     */
    public void reset() {
        this.state = null;
    }

    /**
     * Returns true when status is in OK state.
     */
    public boolean isOk() {
        return this.state == null || this.state.code == 0;
    }

    /**
     * Set error code.
     */
    public void setCode(int code) {
        if (this.state == null) {
            this.state = new State(code, null);
        } else {
            this.state.code = code;
        }
    }

    /**
     * Get error code.
     */
    public int getCode() {
        return this.state == null ? 0 : this.state.code;
    }

    /**
     * Get raft error from error code.
     */
    public RaftError getRaftError() {
        return this.state == null ? RaftError.SUCCESS : RaftError.forNumber(this.state.code);
    }

    /**
     * Set error msg
     */
    public void setErrorMsg(String errMsg) {
        if (this.state == null) {
            this.state = new State(0, errMsg);
        } else {
            this.state.msg = errMsg;
        }
    }

    /**
     * Set error code and error msg.
     */
    public void setError(int code, String fmt, Object... args) {
        this.state = new State(code, String.format(String.valueOf(fmt), args));
    }

    /**
     * Set raft error and error msg.
     */
    public void setError(RaftError error, String fmt, Object... args) {
        this.state = new State(error.getNumber(), String.format(String.valueOf(fmt), args));
    }

    @Override
    public String toString() {
        if (isOk()) {
            return "Status[OK]";
        } else {
            return "Status[" + RaftError.describeCode(this.state.code) + "<" + this.state.code + ">: " + this.state.msg
                   + "]";
        }
    }

    @Override
    public Status copy() {
        return new Status(this.getCode(), this.getErrorMsg());
    }

    /**
     * Get the error msg.
     */
    public String getErrorMsg() {
        return this.state == null ? null : this.state.msg;
    }
}
