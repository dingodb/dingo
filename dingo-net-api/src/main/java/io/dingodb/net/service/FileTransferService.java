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

package io.dingodb.net.service;

import io.dingodb.common.Location;

import java.nio.file.Path;
import java.util.ServiceLoader;

public interface FileTransferService {

    void transfer(Location location, Path source, Path target);

    static FileTransferService getDefault() {
        return ServiceLoader.load(FileTransferService.class).iterator().next();
    }

    static void transferTo(Location location, Path source, Path target) {
        getDefault().transfer(location, source, target);
    }

}
