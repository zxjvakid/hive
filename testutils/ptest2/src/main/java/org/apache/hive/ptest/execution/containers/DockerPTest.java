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

package org.apache.hive.ptest.execution.containers;

import org.apache.hive.ptest.execution.LocalCommandFactory;
import org.apache.hive.ptest.execution.PTest;
import org.apache.hive.ptest.execution.conf.TestConfiguration;
import org.apache.hive.ptest.execution.context.ExecutionContext;
import org.apache.hive.ptest.execution.ssh.RSyncCommandExecutor;
import org.apache.hive.ptest.execution.ssh.SSHCommandExecutor;
import org.slf4j.Logger;

import java.io.File;

public class DockerPTest extends PTest {
  public DockerPTest(TestConfiguration configuration, ExecutionContext executionContext,
      String buildTag, File logDir, LocalCommandFactory localCommandFactory,
      SSHCommandExecutor sshCommandExecutor, RSyncCommandExecutor rsyncCommandExecutor,
      Logger logger) throws Exception {
    super(configuration, executionContext, buildTag, logDir, localCommandFactory,
        sshCommandExecutor, rsyncCommandExecutor, logger);
  }
}
