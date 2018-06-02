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

package org.apache.hive.ptest.api.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hive.ptest.api.request.TestStartRequest;
import org.junit.Test;

public class TestTestStartRequest {
  ObjectMapper mMapper = new ObjectMapper();
  @Test
  public void testJson() throws JsonProcessingException {
    String profile = "ptest";
    String testHandle = "Dummy-Precommit-Test-1";
    String jira = "HIVE-19425";
    String patch = "https://issues.apache.org/jira/secure/attachment/12923111/HIVE-19429.01-ptest.patch";
    boolean clearLibraryCache = false;
    TestStartRequest startRequest = new TestStartRequest(profile, testHandle, jira, patch, clearLibraryCache);
    String payloadString = mMapper.writeValueAsString(startRequest);
    System.out.println(payloadString);
  }
}
