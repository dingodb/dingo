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

package io.dingodb.raft.storage;

import io.dingodb.raft.Lifecycle;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.option.RaftMetaStorageOptions;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface RaftMetaStorage extends Lifecycle<RaftMetaStorageOptions>, Storage {
    /**
     * Set current term.
     */
    boolean setTerm(final long term);

    /**
     * Get current term.
     */
    long getTerm();

    /**
     * Set voted for information.
     */
    boolean setVotedFor(final PeerId peerId);

    /**
     * Get voted for information.
     */
    PeerId getVotedFor();

    /**
     * Set term and voted for information.
     */
    boolean setTermAndVotedFor(final long term, final PeerId peerId);
}
