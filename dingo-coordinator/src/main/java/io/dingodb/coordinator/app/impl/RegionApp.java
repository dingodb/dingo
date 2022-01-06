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

package io.dingodb.coordinator.app.impl;

import io.dingodb.coordinator.GeneralId;
import io.dingodb.coordinator.app.App;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Map;
import java.util.StringJoiner;

@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RegionApp implements App<RegionApp, RegionView> {

    private static final long serialVersionUID = 3308609793292842290L;

    private String regionId;
    private GeneralId appId;

    private byte[] startKey;
    private byte[] endKey;

    private long version;

    private GeneralId view;

    private Map<String, String> labels;

    public RegionApp(String regionId, GeneralId appId, byte[] startKey, byte[] endKey, long version) {
        this.regionId = regionId;
        this.appId = appId;
        this.startKey = startKey;
        this.endKey = endKey;
        this.version = version;
    }

    public String regionId() {
        return regionId;
    }

    public byte[] startKey() {
        return startKey;
    }

    public RegionApp startKey(byte[] startKey) {
        this.startKey = startKey;
        return this;
    }

    public byte[] endKey() {
        return endKey;
    }

    public RegionApp endKey(byte[] endKey) {
        this.endKey = endKey;
        return this;
    }

    public long version() {
        return version;
    }

    public RegionApp version(long version) {
        this.version = version;
        return this;
    }

    public void increaseVersion() {
        version++;
    }

    @Override
    public Map<String, String> labels() {
        return labels;
    }

    @Override
    public GeneralId appId() {
        return appId;
    }

    @Override
    public GeneralId view() {
        return view;
    }

    public RegionApp view(GeneralId view) {
        this.view = view;
        return this;
    }


    @Override
    public String toString() {
        return new StringJoiner(", ", RegionApp.class.getSimpleName() + "[", "]")
            .add("regionId=" + regionId)
            .add("appId=" + appId)
            .add("startKey=" + Arrays.toString(startKey))
            .add("endKey=" + Arrays.toString(endKey))
            .add("version=" + version)
            .add("labels=" + labels)
            .toString();
    }
}
