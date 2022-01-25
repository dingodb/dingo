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

package io.dingodb.store.row.storage;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class KVState {
    private final KVOperation op;
    private final KVStoreClosure done;

    public static KVState of(final KVOperation op, final KVStoreClosure done) {
        return new KVState(op, done);
    }

    public KVState(KVOperation op, KVStoreClosure done) {
        this.op = op;
        this.done = done;
    }

    public boolean isSameOp(final KVOperation o) {
        return this.op.getOp() == o.getOp();
    }

    public KVOperation getOp() {
        return op;
    }

    public byte getOpByte() {
        return this.op.getOp();
    }

    public KVStoreClosure getDone() {
        return done;
    }

    @Override
    public String toString() {
        return "KVState{" + "op=" + op + ", done=" + done + '}';
    }
}
