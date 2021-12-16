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

import com.alipay.sofa.jraft.util.BytesUtil;
import lombok.Getter;
import lombok.Setter;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Getter
@Setter
public class ScanRequest extends BaseRequest {
    private static final long serialVersionUID = -7229126480671449199L;

    private byte[] startKey;
    private byte[] endKey;
    // If limit == 0, it will be modified to Integer.MAX_VALUE on the server
    // and then queried.  So 'limit == 0' means that the number of queries is
    // not limited. This is because serialization uses varint to compress
    // numbers.  In the case of 0, only 1 byte is occupied, and Integer.MAX_VALUE
    // takes 5 bytes.
    private int limit;
    private boolean readOnlySafe = true;
    private boolean returnValue = true;
    private boolean reverse = false;

    @Override
    public byte magic() {
        return SCAN;
    }

    @Override
    public String toString() {
        return "ScanRequest{" + "startKey=" + BytesUtil.toHex(startKey) + ", endKey=" + BytesUtil.toHex(endKey)
               + ", limit=" + limit + ", reverse=" + reverse + ", readOnlySafe=" + readOnlySafe + ", returnValue="
               + returnValue + "} " + super.toString();
    }
}
