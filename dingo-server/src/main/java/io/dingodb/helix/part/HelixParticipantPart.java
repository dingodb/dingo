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

package io.dingodb.helix.part;

import io.dingodb.helix.service.ServiceProvider;
import org.apache.helix.participant.statemachine.StateModel;

import java.util.Map;

public interface HelixParticipantPart<M extends StateModel> extends HelixPart {

    M stateModel(String resource, String partition);

    Map<String, M> stateModel(String resource);

    M stateModel(String resource, String partition, M stateModel);

    ServiceProvider<M> serviceProvider();

}
