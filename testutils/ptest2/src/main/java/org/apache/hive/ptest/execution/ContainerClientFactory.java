/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hive.ptest.execution;

import com.google.common.collect.ImmutableMap;
import org.apache.hive.ptest.api.server.TestLogger;
import org.apache.hive.ptest.execution.containers.DockerClient;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

public abstract class ContainerClientFactory {
  public enum ContainerType {
    DOCKER
  }
  public static ContainerClient get(ContainerType containerType, ContainerClientContext context) throws Exception {
    switch(containerType) {
    case DOCKER:
      return new DockerClient(context);
    default:
      throw new Exception("Unknown container type");
    }
  }

  public static class ContainerClientContext {
    final Logger logger;
    final Map<String, String> templateDefaults;
    public ContainerClientContext(Logger logger, Map<String, String> templateDefaults) {
      this.logger = logger;
      this.templateDefaults = new HashMap<>(templateDefaults);
    }
    public Logger getLogger() {
      return logger;
    }

    public Map<String, String> getTemplateDefaults() {
      return templateDefaults;
    }
  }
}
