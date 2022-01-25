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

package io.dingodb.raft.util;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public abstract class FileOutputSignalHandler implements JRaftSignalHandler {
    protected File getOutputFile(final String path, final String baseFileName) throws IOException {
        makeDir(path);
        final String now = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
        final String fileName = baseFileName + "." + now;
        final File file = Paths.get(path, fileName).toFile();
        if (!file.exists() && !file.createNewFile()) {
            throw new IOException("Fail to create file: " + file);
        }
        return file;
    }

    private static void makeDir(final String path) throws IOException {
        final File dir = Paths.get(path).toFile().getAbsoluteFile();
        if (dir.exists()) {
            Requires.requireTrue(dir.isDirectory(), String.format("[%s] is not directory.", path));
        } else {
            FileUtils.forceMkdir(dir);
        }
    }
}
