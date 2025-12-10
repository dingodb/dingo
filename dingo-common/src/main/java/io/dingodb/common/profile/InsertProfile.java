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

package io.dingodb.common.profile;

import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

@Data
public class InsertProfile extends Profile {
    boolean pessimisticTxn;
    public InsertProfile(String type, boolean pessimisticTxn) {
        super(type);
        start();
        this.pessimisticTxn = pessimisticTxn;
        step1 = new AtomicLong(0);
        step2 = new AtomicLong(0);
        step3 = new AtomicLong(0);
        step4 = new AtomicLong(0);
        step5 = new AtomicLong(0);
        autoInc = new AtomicLong(0);
        typeCheck = new AtomicLong(0);
        encode = new AtomicLong(0);
    }

    AtomicLong step1;

    AtomicLong step2;

    AtomicLong step3;

    AtomicLong step4;

    AtomicLong step5;

    AtomicLong autoInc;

    AtomicLong typeCheck;

    AtomicLong encode;

    public void step1(long start) {
        long sub = System.currentTimeMillis() - start;
        step1.addAndGet(sub);
        count.incrementAndGet();
    }

    public void step2(long start) {
        long sub = System.currentTimeMillis() - start;
        step2.addAndGet(sub);
    }

    public void step3(long start) {
        long sub = System.currentTimeMillis() - start;
        step3.addAndGet(sub);
    }

    public void step4(long start) {
        long sub = System.currentTimeMillis() - start;
        step4.addAndGet(sub);
    }

    public void step5(long start) {
        long sub = System.currentTimeMillis() - start;
        step5.addAndGet(sub);
    }

    public void autoInc(long start) {
        long sub = System.currentTimeMillis() - start;
        autoInc.addAndGet(sub);
    }

    public void typeCheck(long start) {
        long sub = System.currentTimeMillis() - start;
        typeCheck.addAndGet(sub);
    }

    public void encode(long start) {
        long sub = System.currentTimeMillis() - start;
        encode.addAndGet(sub);
    }
}
