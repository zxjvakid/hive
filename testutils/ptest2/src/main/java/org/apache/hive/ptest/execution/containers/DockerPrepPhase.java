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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.hive.ptest.execution.ContainerClient;
import org.apache.hive.ptest.execution.ContainerClientFactory;
import org.apache.hive.ptest.execution.ContainerClientFactory.ContainerClientContext;
import org.apache.hive.ptest.execution.ContainerClientFactory.ContainerType;
import org.apache.hive.ptest.execution.HostExecutor;
import org.apache.hive.ptest.execution.LocalCommandFactory;
import org.apache.hive.ptest.execution.PrepPhase;
import org.slf4j.Logger;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DockerPrepPhase extends PrepPhase {
  private final ContainerClient containerClient;
  public DockerPrepPhase(List<HostExecutor> hostExecutors, LocalCommandFactory localCommandFactory,
      ImmutableMap<String, String> templateDefaults, File scratchDir, File patchFile,
      Logger logger) throws Exception {
    super(hostExecutors, localCommandFactory, templateDefaults, scratchDir, patchFile, logger);
    //TODO create context builder and pass the environment variables to the context
    ContainerClientContext context = new ContainerClientContext(logger, templateDefaults);
    containerClient = ContainerClientFactory.get(ContainerType.DOCKER, context);
  }

  @Override
  public void execute() throws Exception {
    execLocally("rm -rf $workingDir/scratch");
    execLocally("mkdir -p $workingDir/scratch");
    createPatchFiles();
    long start;
    long elapsedTime;
    start = System.currentTimeMillis();
    //TODO give a proper label to the build
    containerClient.defineImage(getLocalScratchDir());
    execLocally(getDockerBuildCommand());
    elapsedTime = TimeUnit.MINUTES.convert((System.currentTimeMillis() - start),
        TimeUnit.MILLISECONDS);
    logger.info("PERF: Docker build image took " + elapsedTime + " minutes");
  }

  @VisibleForTesting
  public String getLocalScratchDir() {
    return mScratchDir.getAbsolutePath();
  }

  @VisibleForTesting
  public String getDockerBuildCommand() throws Exception {
    return containerClient.getBuildCommand(getLocalScratchDir(), 30, TimeUnit.MINUTES);
  }
}
