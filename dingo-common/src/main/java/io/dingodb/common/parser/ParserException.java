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

package io.dingodb.common.parser;

import java.io.Serial;
import java.io.Serializable;

public class ParserException extends RuntimeException implements Serializable {

    @Serial
    private static final long serialVersionUID = -8653547291279578060L;

    private int line;
    private int column;

    public ParserException(){
    }

    public ParserException(String message){
        super(message);
    }



    public ParserException(String message, Throwable e){
        super(message, e);
    }

    public ParserException(String message, int line, int column){
        super(message);
        this.line = line;
        this.column = column;
    }

    public ParserException(Throwable ex, String ksql){
        super("parse error. detail message is :\n" + ex.getMessage() + "\nsource sql is : \n" + ksql, ex);
    }
}
