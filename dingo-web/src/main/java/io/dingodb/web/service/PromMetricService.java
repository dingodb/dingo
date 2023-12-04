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

package io.dingodb.web.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.common.restful.RestfulClient;
import io.dingodb.web.model.dto.PromResponseInfo;
import io.dingodb.web.model.dto.PromResultInfo;
import io.dingodb.web.model.vo.DiskUsageInfo;
import io.dingodb.web.model.vo.ProcessInfo;
import io.dingodb.web.model.vo.ResourceExecutorInfo;
import io.dingodb.web.model.vo.ResourceInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Service
public class PromMetricService {
    public static final String SYSTEM_CPU_IDLE_METRIC= "avg(rate(node_cpu_seconds_total{mode=\"idle\",instance=\"location\"}[2m]))";

    public static final String SYSTEM_MEM_AVAILABLE_METRIC = "node_memory_MemAvailable_bytes";
    public static final String SYSTEM_MEM_TOTAL_METRIC = "node_memory_MemTotal_bytes";

    public static final String SYSTEM_DISK_AVAIL_METRIC = "node_filesystem_avail_bytes{fstype=~\"ext.*|xfs\",mountpoint!~\".*pod.*\"}";
    public static final String SYSTEM_DISK_TOTAL_METRIC = "node_filesystem_size_bytes{fstype=~\"ext.*|xfs\",mountpoint!~\".*pod.*\"}";
    public static final String JVM_HEAP_METRIC = "jvm_memory_bytes_used{area=\"heap\"}";
    public static final String JVM_NON_HEAP_METRIC = "jvm_memory_bytes_used{area=\"nonheap\"}";
    public static final String JVM_MAX_METRIC = "jvm_memory_bytes_max{area=\"heap\"}";

    public static final String PROCESS_CPU_USAGE = "process_cpu_usage";
    public static final String PROCESS_MEMORY_DATA_AND_STACK = "process_memory_data_and_stack";
    public static final String PROCESS_MEMORY_RESIDENT = "process_memory_resident";
    public static final String PROCESS_MEMORY_SHARED = "process_memory_shared";
    public static final String PROCESS_MEMORY_TEXT = "process_memory_text";
    public static final String PROCESS_MEMORY_VIRTUAL = "process_memory_virtual";
    public static final String PROCESS_DISK_WRITE_BYTES_SECOND = "process_disk_write_bytes_second";
    public static final String PROCESS_DISK_READ_BYTES_SECOND = "process_disk_read_bytes_second";

    public static final BigDecimal TMP1 = new BigDecimal(1);
    public static final BigDecimal ZERO = new BigDecimal(0);
    public static final BigDecimal TMP2 = new BigDecimal(100);
    // show MB
    public static final BigDecimal FORMAT_DISPLAY_UNITS = new BigDecimal(1024 * 1024);
    public static final String FORMAT_UNITS = "MB";

    @Value("${server.prometheus}")
    public String basicUrl;

    Map<String, BigDecimal> cpuIdleMap = new HashMap<>();
    Map<String, BigDecimal> memAvailMap;
    Map<String, BigDecimal> memTotalMap;
    Map<String, Map<String, Object>>  diskAvailMap;
    Map<String, Map<String, Object>>  diskTotalMap;
    public Map<String, BigDecimal> jvmHeapMap;
    Map<String, BigDecimal> jvmNonHeapMap;
    Map<String, BigDecimal> jvmMaxMap;

    Map<String, BigDecimal> processCpuUsageMap;
    Map<String, BigDecimal> processDataStackMemMap;
    Map<String, BigDecimal> processResidentMemMap;
    Map<String, BigDecimal> processSharedMemMap;
    Map<String, BigDecimal> processTxtMemMap;
    Map<String, BigDecimal> processVirtualMemMap;
    Map<String, BigDecimal> processDiskWritePerMap;
    Map<String, BigDecimal> processDiskReadPerMap;


    public synchronized void loadMetric(long currentTimeSeconds, boolean refresh) {
        if (refresh || memAvailMap == null) {
            cpuIdleMap.clear();
            memAvailMap = getDistanceMetric(SYSTEM_MEM_AVAILABLE_METRIC, currentTimeSeconds);
            memTotalMap = getDistanceMetric(SYSTEM_MEM_TOTAL_METRIC, currentTimeSeconds);
            jvmHeapMap = getDistanceMetric(JVM_HEAP_METRIC, currentTimeSeconds);
            jvmNonHeapMap = getDistanceMetric(JVM_NON_HEAP_METRIC, currentTimeSeconds);
            jvmMaxMap = getDistanceMetric(JVM_MAX_METRIC, currentTimeSeconds);

            diskAvailMap = getDistanceDiskVal(SYSTEM_DISK_AVAIL_METRIC, currentTimeSeconds);
            diskTotalMap = getDistanceDiskVal(SYSTEM_DISK_TOTAL_METRIC, currentTimeSeconds);
            processCpuUsageMap = getDistanceMetric(PROCESS_CPU_USAGE, currentTimeSeconds);
            processDataStackMemMap = getDistanceMetric(PROCESS_MEMORY_DATA_AND_STACK, currentTimeSeconds);
            processResidentMemMap = getDistanceMetric(PROCESS_MEMORY_RESIDENT, currentTimeSeconds);
            processSharedMemMap = getDistanceMetric(PROCESS_MEMORY_SHARED, currentTimeSeconds);
            processTxtMemMap = getDistanceMetric(PROCESS_MEMORY_TEXT, currentTimeSeconds);
            processVirtualMemMap = getDistanceMetric(PROCESS_MEMORY_VIRTUAL, currentTimeSeconds);
            processDiskWritePerMap = getDistanceMetric(PROCESS_DISK_WRITE_BYTES_SECOND, currentTimeSeconds);
            processDiskReadPerMap = getDistanceMetric(PROCESS_DISK_READ_BYTES_SECOND, currentTimeSeconds);
        }
    }

    public double getCpuUsage(String instance, long currentTimeSeconds) {
        BigDecimal idle = cpuIdleMap.computeIfAbsent(instance, location -> {
            String realCpuUsageMetric = SYSTEM_CPU_IDLE_METRIC.replace("location", location);
            PromResponseInfo promResponseInfo = getPromByMetric(realCpuUsageMetric, currentTimeSeconds);
            if (promResponseInfo == null) {
               return ZERO;
            } else {
               Object val = promResponseInfo.getData().getResult().get(0).getValue().get(1);
               return new BigDecimal(val.toString());
            }
        });
        if (idle == ZERO) {
            return 0;
        }
        return TMP1.subtract(idle).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

    public PromResponseInfo getPromByMetric(String metric, long currentTimeSeconds) {
        String url = basicUrl + "?time=" + currentTimeSeconds + "&query=" + metric;
        RestfulClient restfulClient = new RestfulClient();
        String res = restfulClient.get(url);
        if (res == null) {
            return null;
        }
        try {
            ObjectMapper objectMapperRes = new ObjectMapper();
            PromResponseInfo promResponseInfo = objectMapperRes.readValue(res, PromResponseInfo.class);
            if (promResponseInfo.getStatus().equals("success")) {
                if (promResponseInfo.getData().getResult() == null
                    || promResponseInfo.getData().getResult().size() == 0) {
                    return null;
                } else {
                    return promResponseInfo;
                }
            } else {
                return null;
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public double getMemUsage(String instance) {
        BigDecimal available = memAvailMap.getOrDefault(instance, ZERO);

        BigDecimal total = memTotalMap.getOrDefault(instance, ZERO);
        if (total == ZERO || available == ZERO) {
            return 0;
        }
        return  (TMP1.subtract(available.divide(total, 2, RoundingMode.HALF_UP))).doubleValue();
    }

    public Map<String, BigDecimal> getDistanceMetric(String metric, long currentTimeSeconds) {
        PromResponseInfo promResponseInfo = getPromByMetric(metric, currentTimeSeconds);
        if (promResponseInfo == null) {
            return new HashMap<>();
        }
        return promResponseInfo.getData().getResult()
            .stream()
            .collect(Collectors.toMap(
                p -> p.getMetric().getInstance(),
                p -> new BigDecimal(p.getValue().get(1).toString())));
    }

    public ProcessInfo getProcessMetric(String instance) {
        double processCpuUsage = processCpuUsageMap.getOrDefault(instance, ZERO).doubleValue();
        double dataStackMem = processDataStackMemMap.getOrDefault(instance, ZERO)
            .divide(FORMAT_DISPLAY_UNITS, 2, RoundingMode.HALF_UP).doubleValue();
        double residentMem = processResidentMemMap.getOrDefault(instance, ZERO)
            .divide(FORMAT_DISPLAY_UNITS, 2, RoundingMode.HALF_UP).doubleValue();
        double sharedMem = processSharedMemMap.getOrDefault(instance, ZERO)
            .divide(FORMAT_DISPLAY_UNITS, 2, RoundingMode.HALF_UP).doubleValue();
        double txtMem = processTxtMemMap.getOrDefault(instance, ZERO)
            .divide(FORMAT_DISPLAY_UNITS, 2, RoundingMode.HALF_UP).doubleValue();
        double virtualMem = processVirtualMemMap.getOrDefault(instance, ZERO)
            .divide(FORMAT_DISPLAY_UNITS, 2, RoundingMode.HALF_UP).doubleValue();
        double diskWritePer = processDiskWritePerMap.getOrDefault(instance, ZERO).doubleValue();
        double diskReadPer = processDiskReadPerMap.getOrDefault(instance, ZERO).doubleValue();
        return new ProcessInfo(
            processCpuUsage + "%",
            dataStackMem + FORMAT_UNITS,
            residentMem + FORMAT_UNITS,
            sharedMem + FORMAT_UNITS,
            txtMem + FORMAT_UNITS,
            virtualMem + FORMAT_UNITS,
            diskWritePer + "bytes/second",
            diskReadPer + "bytes/second");
    }

    public double getJvmUsage(String instance) {
        BigDecimal jvmHeap = jvmHeapMap.getOrDefault(instance, ZERO);

        BigDecimal jvmMax = jvmMaxMap.getOrDefault(instance, ZERO);
        if (jvmMax == ZERO || jvmHeap == ZERO) {
            return 0;
        }
        return jvmHeap.divide(jvmMax, 2, RoundingMode.HALF_UP).doubleValue();
    }

    public BigDecimal getNonHeap(String instance) {
        return jvmNonHeapMap.getOrDefault(instance, ZERO);
    }

    public ResourceExecutorInfo getResourceExecutorInfo(String instance,
                                                        ResourceInfo resourceInfo) {
        double jvmUsage = getJvmUsage(instance);
        BigDecimal nonHeap = getNonHeap(instance);
        double nonHeapVal = nonHeap.divide(FORMAT_DISPLAY_UNITS, 2, RoundingMode.HALF_UP).doubleValue();
        return new ResourceExecutorInfo(
            resourceInfo.getCpuUsage(),
            resourceInfo.getMemUsage(),
            resourceInfo.getDiskUsage(),
            jvmUsage,
            nonHeapVal + "MB");
    }

    public List<DiskUsageInfo> getDiskSpaceUsage(String instance) {

        Map<String, Object> availMap
            = diskAvailMap.get(instance);


        Map<String, Object> totalMap
            = diskTotalMap.get(instance);

        if (availMap == null || totalMap == null) {
            return new ArrayList<>();
        }

        return availMap.entrySet().stream().map(e -> {
            if (totalMap.containsKey(e.getKey())) {
                BigDecimal avail = new BigDecimal(e.getValue().toString());
                BigDecimal total = new BigDecimal(totalMap.get(e.getKey()).toString());
                BigDecimal diskUsage = TMP1.subtract(avail.divide(total, 2, RoundingMode.HALF_UP)).multiply(TMP2);
                return new DiskUsageInfo(e.getKey(), diskUsage.doubleValue());
            } else {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    public Map<String, Map<String, Object>> getDistanceDiskVal(String metric, long currentTimeSeconds) {
        PromResponseInfo promResponseInfo = getPromByMetric(metric, currentTimeSeconds);
        if (promResponseInfo == null) {
            return new HashMap<>();
        }
        Map<String, List<PromResultInfo>> resultMap = promResponseInfo.getData().getResult()
            .stream()
            .collect(Collectors.groupingBy(e -> e.getMetric().getInstance()));
        return resultMap.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().stream()
                    .collect(Collectors.toMap(
                        value -> value.getMetric().getMountpoint(),
                        value -> value.getValue().get(1)
                    ))
            ));
    }

    public ResourceInfo getResource(String instance, long currentTimeSeconds) {
        try {
            double cpuUsage = getCpuUsage(instance, currentTimeSeconds);
            double memUsage = getMemUsage(instance);
            List<DiskUsageInfo> diskInfoList = getDiskSpaceUsage(instance);

            return new ResourceInfo(cpuUsage, memUsage, diskInfoList);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new ResourceInfo(0, 0, new ArrayList<>());
        }
    }
}
