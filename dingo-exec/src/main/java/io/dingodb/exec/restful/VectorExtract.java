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
    private static final RestfulConnection connection = new RestfulConnection();
    private static final Map<String, RestfulRequest> vectorInterface = new HashMap<>();

    static {
        vectorInterface.put(VectorImageFun.NAME,
            new RestfulRequest("http://host:port/img2vec", null, "PUT", ""));
        vectorInterface.put(VectorTextFun.NAME,
            new RestfulRequest("http://host:port/text2vec", null, "PUT", ""));
    }

    public static Float[] getTxtVector(String funName, String host, Object param) {
        host = removeQuote(host);
        RestfulRequest request = vectorInterface.get(funName.toLowerCase());
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("text_str", param.toString());
        return getRestFulFloats(host, request, paramMap);
    }

    public static Float[] getImgVector(String funName, String host, Object url, boolean localPath) {
        host = removeQuote(host);
        RestfulRequest request = vectorInterface.get(funName.toLowerCase());
        Map<String, Object> paramMap = new HashMap<>();
        String imgUrl = url.toString();
        imgUrl = removeQuote(imgUrl);
        paramMap.put("img_url", imgUrl);
        paramMap.put("local_path", localPath);
        return getRestFulFloats(host, request, paramMap);
    }

    private static String removeQuote(String param) {
        if (param == null) {
            throw new RuntimeException("vector load param error");
        }
        if (param.contains("'")) {
            param = param.replace("'", "");
        }
        return param;
    }

    private static Float[] getRestFulFloats(String host, RestfulRequest request, Map<String, Object> paramMap) {
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
