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

package io.dingodb.exec.fun.mysql;

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.NullaryOp;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class VersionFun extends NullaryOp {
    public static final VersionFun INSTANCE = new VersionFun();
    public static final String NAME = "version";

    private static final long serialVersionUID = -4130064040675181327L;

    @Override
    public Object eval(EvalContext context, ExprConfig config) {
        String command = "git describe --tag --long --dirty=M --always";
        String version;
        BufferedReader reader;
        try {
            Process process = Runtime.getRuntime().exec(command);
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                log.debug("Failed to get current release version");
            } else {
                reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                if ((version = reader.readLine()) != null) {
                    return version;
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
