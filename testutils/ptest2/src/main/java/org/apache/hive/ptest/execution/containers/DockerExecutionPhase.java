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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hive.ptest.execution.ContainerClient;
import org.apache.hive.ptest.execution.ContainerClientFactory;
import org.apache.hive.ptest.execution.ContainerClientFactory.ContainerClientContext;
import org.apache.hive.ptest.execution.ContainerClientFactory.ContainerType;
import org.apache.hive.ptest.execution.ExecutionPhase;
import org.apache.hive.ptest.execution.HostExecutor;
import org.apache.hive.ptest.execution.HostExecutorBuilder;
import org.apache.hive.ptest.execution.LocalCommandFactory;
import org.apache.hive.ptest.execution.conf.TestBatch;
import org.apache.hive.ptest.execution.context.ExecutionContext;
import org.apache.hive.ptest.execution.ssh.RemoteCommandResult;
import org.slf4j.Logger;

import java.io.File;
import java.util.List;
import java.util.Set;

public class DockerExecutionPhase extends ExecutionPhase {
  private final ContainerClient dockerClient;
  public DockerExecutionPhase(List<HostExecutor> hostExecutors, ExecutionContext executionContext,
      HostExecutorBuilder hostExecutorBuilder, LocalCommandFactory localCommandFactory,
      ImmutableMap<String, String> templateDefaults, File succeededLogDir, File failedLogDir,
      Supplier<List<TestBatch>> testBatchSupplier, Set<String> executedTests,
      Set<String> failedTests, Logger logger) throws Exception {
    super(hostExecutors, executionContext, hostExecutorBuilder, localCommandFactory,
        templateDefaults, succeededLogDir, failedLogDir, testBatchSupplier, executedTests,
        failedTests, logger);
    ContainerClientContext context = new ContainerClientContext(logger, templateDefaults);
    dockerClient = ContainerClientFactory.get(ContainerType.DOCKER, context);
  }

  @Override
  public List<TestBatch> getTestBatches() {
    List<TestBatch> testBatches = Lists.newArrayList();
    //Docker containers should be able to run all the batches in parallel
    for (TestBatch batch : testBatchSupplier.get()) {
      testBatches.add(batch);
      parallelWorkQueue.add(batch);
    }
    return testBatches;
  }

  @Override
  protected List<RemoteCommandResult> initalizeHosts()
      throws Exception {
    //TODO kill docker containers in case they are running here
    return Lists.newArrayList();
  }
}
