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

package io.dingodb.exec.restful;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.exec.fun.vector.VectorImageFun;
import io.dingodb.exec.fun.vector.VectorTextFun;

import java.util.HashMap;
import java.util.Map;

public class VectorExtract {
    private static RestfulConnection connection = new RestfulConnection();
    private static Map<String, RestfulRequest> vectorInterface = new HashMap<>();

    static {
        vectorInterface.put(VectorImageFun.NAME, new RestfulRequest("", null, "PUT", ""));
        vectorInterface.put(VectorTextFun.NAME,
            new RestfulRequest("http://host:8080/text2vec", null, "PUT", ""));
    }

    public static Float[] getVector(String funName, String host, Object param) {
        if (host.contains("'")) {
            host = host.replace("'", "");
        }
        RestfulRequest request = vectorInterface.get(funName.toLowerCase());
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put("text_str", param.toString());
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            request.setParam(objectMapper.writeValueAsString(paramMap));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        request.setHost(host);
        String response = connection.getVal(request);
        if (response == null) {
            throw new RuntimeException("vector load error");
        }
        ObjectMapper objectMapperRes = new ObjectMapper();
        try {
            return objectMapperRes.readValue(response, Float[].class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
