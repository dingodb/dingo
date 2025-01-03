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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.profile.Profile;
import io.dingodb.common.profile.SourceProfile;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;

@JsonTypeName("scan0")
@JsonPropertyOrder({
    "tableId",
    "schema",
    "keyMapping",
})
public class ScanParam extends AbstractParams {
    @Getter
    @JsonProperty("table")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    protected final CommonId tableId;
    @Getter
    @JsonProperty("schema")
    protected final DingoType schema;
    @Getter
    @JsonProperty("keyMapping")
    protected final TupleMapping keyMapping;

    @Getter
    protected List<Profile> profileList;
    protected int schemaVersion;
    protected int codecVersion;

    public ScanParam(
        CommonId tableId,
        @NonNull DingoType schema,
        TupleMapping keyMapping,
        int schemaVersion,
        int codecVersion
    ) {
        super(null, null);
        this.tableId = tableId;
        this.schema = schema;
        this.keyMapping = keyMapping;
        this.schemaVersion = schemaVersion;
        this.codecVersion = codecVersion;
    }

    public KeyValueCodec getCodec() {
        return CodecService.getDefault().createKeyValueCodec(codecVersion, schemaVersion, schema, keyMapping);
    }

    public synchronized OperatorProfile getProfile(String type) {
        OperatorProfile profile1 = new OperatorProfile(type);
        profile1.start();
        if (profileList == null) {
            profileList = new ArrayList<>();
            profileList.add(profile1);
        } else {
            if (profileList.size() == 1 && "scanBase".equals(profileList.get(0).getType())) {
                profileList.get(0).getChildren().add(profile1);
            }
        }
        return profile1;
    }

    public synchronized SourceProfile getSourceProfile(String type) {
        if (profileList == null) {
            profileList = new ArrayList<>();
        }
        SourceProfile profile1 = new SourceProfile(type);
        profile1.start();
        profileList.add(profile1);
        return profile1;
    }

    public synchronized void removeProfile(Profile profile) {
        if (profileList != null) {
            profileList.remove(profile);
        }
    }
}
