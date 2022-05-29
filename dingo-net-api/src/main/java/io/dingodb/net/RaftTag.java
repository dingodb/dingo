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

package io.dingodb.net;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.nio.charset.StandardCharsets;

@Getter
@Builder
@EqualsAndHashCode
@AllArgsConstructor
public class RaftTag implements Tag {

    public static final Tag APPENDENTRIES_REQUEST = RaftTag.builder()
        .tag("AppendEntries_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag APPENDENTRIES_RESPONSE = RaftTag.builder()
        .tag("AppendEntries_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag GETFILE_REQUEST = RaftTag.builder()
        .tag("GetFile_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag GETFILE_RESPONSE = RaftTag.builder()
        .tag("GetFile_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag INSTALLSNAPSHOT_REQUEST = RaftTag.builder()
        .tag("InstallSnapshot_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag INSTALLSNAPSHOT_RESPONSE = RaftTag.builder()
        .tag("InstallSnapshot_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag REQUESTVOTE_REQUEST = RaftTag.builder()
        .tag("REQUESTVOTE_REQUEST".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag REQUESTVOTE_RESPONSE = RaftTag.builder()
        .tag("RequestVote_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag PING_REQUEST = RaftTag.builder()
        .tag("Ping_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag PING_RESPONSE = RaftTag.builder()
        .tag("Ping_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag TIMEOUTNOW_REQUEST = RaftTag.builder()
        .tag("TimeoutNow_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag TIMEOUTNOW_RESPONSE = RaftTag.builder()
        .tag("TimeoutNow_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag READINDEX_REQUEST = RaftTag.builder()
        .tag("ReadIndex_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag READINDEX_RESPONSE = RaftTag.builder()
        .tag("ReadIndex_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag ADDPEER_REQUEST = RaftTag.builder()
        .tag("AddPeer_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag ADDPEER_RESPONSE = RaftTag.builder()
        .tag("AddPeer_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag REMOVEPEER_REQUEST = RaftTag.builder()
        .tag("RemovePeer_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag REMOVEPEER_RESPONSE = RaftTag.builder()
        .tag("RemovePeer_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag RESETPEER_REQUEST = RaftTag.builder()
        .tag("ResetPeer_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag RESETPEER_RESPONSE = RaftTag.builder()
        .tag("ResetPeer_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag CHANGEPEERS_REQUEST = RaftTag.builder()
        .tag("ChangePeers_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag CHANGEPEERS_RESPONSE = RaftTag.builder()
        .tag("ChangePeers_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag GETLEADER_REQUEST = RaftTag.builder()
        .tag("GetLeader_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag GETLEADER_RESPONSE = RaftTag.builder()
        .tag("GetLeader_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag SNAPSHOT_REQUEST = RaftTag.builder()
        .tag("Snapshot_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag SNAPSHOT_RESPONSE = RaftTag.builder()
        .tag("Snapshot_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag TRANSFERLEADER_REQUEST = RaftTag.builder()
        .tag("TransferLeader_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag TRANSFERLEADER_RESPONSE = RaftTag.builder()
        .tag("TransferLeader_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag GETPEERS_REQUEST = RaftTag.builder()
        .tag("GetPeers_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag GETPEERS_RESPONSE = RaftTag.builder()
        .tag("GetPeers_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag ADDLEARNERS_REQUEST = RaftTag.builder()
        .tag("AddLearners_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag ADDLEARNERS_RESPONSE = RaftTag.builder()
        .tag("AddLearners_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag REMOVELEARNERS_REQUEST = RaftTag.builder()
        .tag("RemoveLearners_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag REMOVELEARNERS_RESPONSE = RaftTag.builder()
        .tag("RemoveLearners_response".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag RESETLEARNERS_REQUEST = RaftTag.builder()
        .tag("ResetLearners_request".getBytes(StandardCharsets.UTF_8))
        .build();

    public static final Tag RESETLEARNERS_RESPONSE = RaftTag.builder()
        .tag("ResetLearners_response".getBytes(StandardCharsets.UTF_8))
        .build();

    private byte[] tag;

    @Override
    public byte[] toBytes() {
        return tag;
    }

    @Override
    public RaftTag load(byte[] bytes) {
        this.tag = bytes;
        return this;
    }

    @Override
    public int flag() {
        return 2;
    }

    @Override
    public String toString() {
        return new String(tag, StandardCharsets.UTF_8);
    }
}
