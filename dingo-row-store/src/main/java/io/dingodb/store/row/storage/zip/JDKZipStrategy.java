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

package io.dingodb.store.row.storage.zip;

import io.dingodb.store.row.util.ZipUtil;

import java.util.zip.Checksum;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class JDKZipStrategy implements ZipStrategy {
    @Override
    public void compress(final String rootDir, final String sourceDir, final String outputZipFile,
                         final Checksum checksum) throws Throwable {
        ZipUtil.compress(rootDir, sourceDir, outputZipFile, checksum);
    }

    @Override
    public void deCompress(final String sourceZipFile, final String outputDir, final Checksum checksum)
                                                                                                       throws Throwable {
        ZipUtil.decompress(sourceZipFile, outputDir, checksum);
    }
}
