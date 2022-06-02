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

package io.dingodb.raft.rpc.dingo;

public class Tags {
    public static final String APPENDENTRIES_REQUEST = "AppendEntries_request";

    public static final String APPENDENTRIES_RESPONSE = "AppendEntries_response";

    public static final String GETFILE_REQUEST = "GetFile_request";

    public static final String GETFILE_RESPONSE = "GetFile_response";

    public static final String INSTALLSNAPSHOT_REQUEST = "InstallSnapshot_request";

    public static final String INSTALLSNAPSHOT_RESPONSE = "InstallSnapshot_response";

    public static final String REQUESTVOTE_REQUEST = "REQUESTVOTE_REQUEST";

    public static final String REQUESTVOTE_RESPONSE = "RequestVote_response";

    public static final String PING_REQUEST = "Ping_request";

    public static final String PING_RESPONSE = "Ping_response";

    public static final String TIMEOUTNOW_REQUEST = "TimeoutNow_request";

    public static final String TIMEOUTNOW_RESPONSE = "TimeoutNow_response";

    public static final String READINDEX_REQUEST = "ReadIndex_request";

    public static final String READINDEX_RESPONSE = "ReadIndex_response";

    public static final String ADDPEER_REQUEST = "AddPeer_request";

    public static final String ADDPEER_RESPONSE = "AddPeer_response";

    public static final String REMOVEPEER_REQUEST = "RemovePeer_request";

    public static final String REMOVEPEER_RESPONSE = "RemovePeer_response";

    public static final String RESETPEER_REQUEST = "ResetPeer_request";

    public static final String RESETPEER_RESPONSE = "ResetPeer_response";

    public static final String CHANGEPEERS_REQUEST = "ChangePeers_request";

    public static final String CHANGEPEERS_RESPONSE = "ChangePeers_response";

    public static final String GETLEADER_REQUEST = "GetLeader_request";

    public static final String GETLEADER_RESPONSE = "GetLeader_response";

    public static final String SNAPSHOT_REQUEST = "Snapshot_request";

    public static final String SNAPSHOT_RESPONSE = "Snapshot_response";

    public static final String TRANSFERLEADER_REQUEST = "TransferLeader_request";

    public static final String TRANSFERLEADER_RESPONSE = "TransferLeader_response";

    public static final String GETPEERS_REQUEST = "GetPeers_request";

    public static final String GETPEERS_RESPONSE = "GetPeers_response";

    public static final String ADDLEARNERS_REQUEST = "AddLearners_request";

    public static final String ADDLEARNERS_RESPONSE = "AddLearners_response";

    public static final String REMOVELEARNERS_REQUEST = "RemoveLearners_request";

    public static final String REMOVELEARNERS_RESPONSE = "RemoveLearners_response";

    public static final String RESETLEARNERS_REQUEST = "ResetLearners_request";

    public static final String RESETLEARNERS_RESPONSE = "ResetLearners_response";
}
