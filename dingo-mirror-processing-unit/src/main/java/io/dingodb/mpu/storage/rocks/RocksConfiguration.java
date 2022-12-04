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

package io.dingodb.mpu.storage.rocks;

import io.dingodb.common.config.DingoConfiguration;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class RocksConfiguration {

    public static RocksConfiguration refreshRocksConfiguration() {
        RocksConfiguration tmpInst;
        try {
            DingoConfiguration.instance().setStore(RocksConfiguration.class);
            tmpInst  = DingoConfiguration.instance().getStore();
        } catch (Exception e) {
            tmpInst = new RocksConfiguration();
        }
        return tmpInst;
    }

    private String dbPath;
    private String dbRocksOptionsFile;
    private String logRocksOptionsFile;
    private String bypassSaveInstruction;
    private String bypassWriteDb;

    private ColumnFamilyConfiguration dcfConfiguration;
    private ColumnFamilyConfiguration mcfConfiguration;
    private ColumnFamilyConfiguration icfConfiguration;

    public String dbPath() {
        return dbPath;
    }

    public String dbRocksOptionsFile() {
        return dbRocksOptionsFile;
    }

    public String logRocksOptionsFile() {
        return logRocksOptionsFile;
    }

    public ColumnFamilyConfiguration dcfConfiguration() {
        if (dcfConfiguration == null) {
            return new ColumnFamilyConfiguration();
        }
        return dcfConfiguration;
    }

    public ColumnFamilyConfiguration mcfConfiguration() {
        if (mcfConfiguration == null) {
            return new ColumnFamilyConfiguration();
        }
        return mcfConfiguration;
    }

    public ColumnFamilyConfiguration icfConfiguration() {
        if (icfConfiguration == null) {
            return new ColumnFamilyConfiguration();
        }
        return icfConfiguration;
    }

    public boolean bypassSaveInstruction() {
        if (bypassSaveInstruction == null) {
            return false;
        } else {
            return bypassSaveInstruction.equalsIgnoreCase("ABSOLUTELY_YES");
        }
    }

    public boolean bypassWriteDb() {
        if (bypassWriteDb == null) {
            return false;
        } else {
            return bypassWriteDb.equalsIgnoreCase("ABSOLUTELY_YES");
        }
    }
}

