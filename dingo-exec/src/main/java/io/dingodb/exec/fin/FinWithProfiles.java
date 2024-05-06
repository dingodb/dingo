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

package io.dingodb.exec.fin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.profile.Profile;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.AbstractParams;
import io.dingodb.expr.json.runtime.Parser;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

@Slf4j
@Data
public class FinWithProfiles implements Fin {
    public static final Parser PARSER = Parser.JSON;

    @JsonProperty("profile")
    Profile profile;

    public FinWithProfiles() {

    }

    public FinWithProfiles(Profile profile) {
        if (profile != null) {
            this.profile = profile;
        } else {
            this.profile = new Profile("base");
        }
    }

    public static FinWithProfiles deserialize(ByteArrayInputStream is) throws IOException {
        return PARSER.parse(is, FinWithProfiles.class);
    }

    public static @NonNull FinWithProfiles of(Profile profile) {
        return new FinWithProfiles(profile);
    }

    public synchronized List<Profile> getProfiles() {
        return profile.getChildren();
    }

    @Override
    public void writeStream(@NonNull OutputStream os) throws IOException {
        PARSER.writeStream(os, this);
    }

    @Override
    public String detail() {
        return "";
    }

    @Override
    public String toString() {
        try {
            return PARSER.stringify(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void addProfileList(List<Profile> profileList) {
        profile.getChildren().addAll(profileList);
    }

    public synchronized void addProfile(Profile profile) {
        if (profile != null) {
            if (profile.getEnd() == 0) {
                profile.end();
            }

            if (profile.getChildren().isEmpty() && this.profile != null) {
                if (profile.getType().equalsIgnoreCase(this.profile.getType())) {
                    OperatorProfile serial = new OperatorProfile(profile.getType());
                    serial.getChildren().add(profile);

                    boolean r = this.profile.getChildren().stream()
                     .allMatch(p -> p.getType().equalsIgnoreCase(this.profile.getType()));
                    if (!r) {
                        serial.getChildren().add(this.profile);
                    } else {
                        serial.getChildren().addAll(this.profile.getChildren());
                    }
                    profile = serial;
                } else {
                    profile.getChildren().add(this.profile);
                }
            } else if (!profile.getChildren().isEmpty() && this.profile != null) {
                profile.getChildren().add(this.profile);
            }
            this.profile = profile;
        }
    }

    public synchronized void addProfile(Vertex vertex) {
        AbstractParams param = vertex.getParam();
        addProfile(param.getProfile());
    }

    public void setProfile(Profile profile) {
        this.profile = profile;
    }
}
