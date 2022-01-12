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

import com.alipay.sofa.jraft.Status;
import io.dingodb.store.row.errors.Errors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Getter
@AllArgsConstructor
public class KVClosureAdapter implements KVStoreClosure {
    private static final Logger LOG = LoggerFactory.getLogger(KVClosureAdapter.class);

    private KVStoreClosure done;
    private KVOperation operation;

    @Override
    public void run(final Status status) {
        if (status.isOk()) {
            setError(Errors.NONE);
        } else {
            LOG.error("Fail status: {}.", status);
            if (getError() == null) {
                switch (status.getRaftError()) {
                    case SUCCESS:
                        setError(Errors.NONE);
                        break;
                    case EINVAL:
                        setError(Errors.INVALID_REQUEST);
                        break;
                    case EIO:
                        setError(Errors.STORAGE_ERROR);
                        break;
                    default:
                        setError(Errors.LEADER_NOT_AVAILABLE);
                        break;
                }
            }
        }
        if (done != null) {
            done.run(status);
        }
        reset();
    }

    @Override
    public Errors getError() {
        if (this.done != null) {
            return this.done.getError();
        }
        return null;
    }

    @Override
    public void setError(Errors error) {
        if (this.done != null) {
            this.done.setError(error);
        }
    }

    @Override
    public Object getData() {
        if (this.done != null) {
            return this.done.getData();
        }
        return null;
    }

    @Override
    public void setData(Object data) {
        if (this.done != null) {
            this.done.setData(data);
        }
    }

    private void reset() {
        done = null;
        operation = null;
    }
}
