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

package io.dingodb.web.model.vo;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class ResourceExecutorInfo extends ResourceInfo{
    private double heapUsage;
    private String nonHeapSize;

    public ResourceExecutorInfo(double cpuUsage,
                                double memUsage,
                                List<DiskUsageInfo> diskUsage,
                                double heapUsage,
                                String nonHeapSize) {
        super(cpuUsage, memUsage, diskUsage);
        this.heapUsage = heapUsage;
        this.nonHeapSize = nonHeapSize;
    }
}
