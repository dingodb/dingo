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

package io.dingodb.common.util;

import java.io.IOException;
import java.nio.file.Path;

public class Files {

    private Files() {
    }

    public static void createDirectories(Path path) {
        try {
            java.nio.file.Files.createDirectories(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteIfExists(Path path) {
        try {
            if (java.nio.file.Files.isDirectory(path)) {
                java.nio.file.Files.list(path).forEach(Files::deleteIfExists);
                java.nio.file.Files.deleteIfExists(path);
            }
            java.nio.file.Files.deleteIfExists(path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
