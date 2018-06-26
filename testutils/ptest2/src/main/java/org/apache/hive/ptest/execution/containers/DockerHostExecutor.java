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

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.lang.StringUtils;
import org.apache.hive.ptest.execution.AbortDroneException;
import org.apache.hive.ptest.execution.Constants;
import org.apache.hive.ptest.execution.ContainerClient;
import org.apache.hive.ptest.execution.ContainerClientFactory;
import org.apache.hive.ptest.execution.ContainerClientFactory.ContainerClientContext;
import org.apache.hive.ptest.execution.ContainerClientFactory.ContainerType;
import org.apache.hive.ptest.execution.Dirs;
import org.apache.hive.ptest.execution.Drone;
import org.apache.hive.ptest.execution.HostExecutor;
import org.apache.hive.ptest.execution.Templates;
import org.apache.hive.ptest.execution.conf.Host;
import org.apache.hive.ptest.execution.conf.TestBatch;
import org.apache.hive.ptest.execution.ssh.RSyncCommand;
import org.apache.hive.ptest.execution.ssh.RSyncCommandExecutor;
import org.apache.hive.ptest.execution.ssh.RSyncResult;
import org.apache.hive.ptest.execution.ssh.RemoteCommandResult;
import org.apache.hive.ptest.execution.ssh.SSHCommand;
import org.apache.hive.ptest.execution.ssh.SSHCommandExecutor;
import org.apache.hive.ptest.execution.ssh.SSHExecutionException;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DockerHostExecutor extends HostExecutor {
  private final ContainerClientContext containerClientContext;
  private final ContainerClient dockerClient;
  private final int numParallelContainersPerHost;
  private AtomicInteger containerNameId = new AtomicInteger(0);

  public DockerHostExecutor(Host host, String privateKey, ListeningExecutorService executor,
      SSHCommandExecutor sshCommandExecutor, RSyncCommandExecutor rsyncCommandExecutor,
      ImmutableMap<String, String> templateDefaults, File scratchDir, File succeededLogDir,
      File failedLogDir, long numPollSeconds, boolean fetchLogsForSuccessfulTests, Logger logger)
      throws Exception {
    super(host, privateKey, executor, sshCommandExecutor, rsyncCommandExecutor, templateDefaults,
        scratchDir, succeededLogDir, failedLogDir, numPollSeconds, fetchLogsForSuccessfulTests,
        logger);
    containerClientContext = new ContainerClientContext(logger, templateDefaults);
    dockerClient = ContainerClientFactory.get(ContainerType.DOCKER, containerClientContext);
    //TODO get this value from executionContext
    numParallelContainersPerHost = 2;
  }

  @Override
  protected void executeTests(final BlockingQueue<TestBatch> parallelWorkQueue,
      final BlockingQueue<TestBatch> isolatedWorkQueue, final Set<TestBatch> failedTestResults)
      throws Exception {
    if(mShutdown) {
      mLogger.warn("Shutting down host " + mHost.getName());
      return;
    }
    mLogger.info("Starting parallel execution on " + mHost.getName() + " using dockers");
    List<ListenableFuture<Void>> containerResults = Lists.newArrayList();
    for(int containerSlotId = 0; containerSlotId < numParallelContainersPerHost; containerSlotId++) {
      final int finalContainerSlotId = containerSlotId;
      containerResults.add(mExecutor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          TestBatch batch = null;
          Stopwatch sw = Stopwatch.createUnstarted();
          try {
            do {
              batch = parallelWorkQueue.poll(mNumPollSeconds, TimeUnit.SECONDS);
              if(mShutdown) {
                mLogger.warn("Shutting down host " + mHost.getName());
                return null;
              }
              if(batch != null) {
                numParallelBatchesProcessed++;
                sw.reset().start();
                try {
                  if (!executeTestBatch(batch, finalContainerSlotId, failedTestResults)) {
                    failedTestResults.add(batch);
                  }
                } finally {
                  sw.stop();
                  mLogger.info("Finished processing parallel batch [{}] on host {}. ElapsedTime(ms)={}",
                      new Object[]{batch.getName(),mHost.toShortString(), sw.elapsed(TimeUnit.MILLISECONDS)});
                }
              }
            } while(!mShutdown && !parallelWorkQueue.isEmpty());
          } catch(AbortContainerException ex) {
            mLogger.error("Aborting container during parallel execution", ex);
            if(batch != null) {
              Preconditions.checkState(parallelWorkQueue.add(batch),
                  "Could not add batch to parallel queue " + batch);
            }
          }
          return null;
        }
      }));
    }
    if(mShutdown) {
      mLogger.warn("Shutting down host " + mHost.getName());
      return;
    }
    Futures.allAsList(containerResults).get();
  }

  private boolean executeTestBatch(TestBatch batch, int containerSlotId, Set<TestBatch> failedTestResults)
      throws AbortContainerException, IOException, SSHExecutionException {
    final int containerInstanceId = containerNameId.getAndIncrement();
    final String containerName = getContainerName(containerInstanceId);
    String runCommand = dockerClient.getRunContainerCommand(containerName, batch);
    Stopwatch sw = Stopwatch.createStarted();
    mLogger.info("Executing " + batch + " with " + runCommand);
    RemoteCommandResult sshResult = new SSHCommand(mSSHCommandExecutor, mPrivateKey, mHost.getUser(),
        mHost.getName(), containerInstanceId, runCommand, true).
        call();
    sw.stop();
    mLogger.info(
        "Completed executing tests for batch [{}] on host {} using container instance {}. ElapsedTime(ms)={}",
        new Object[] { batch.getName(), mHost.toShortString(), containerInstanceId,
            sw.elapsed(TimeUnit.MILLISECONDS) });

    File batchLogDir = null;
    if (sshResult.getExitCode() == Constants.EXIT_CODE_UNKNOWN) {
      throw new AbortContainerException(
          "Container " + containerInstanceId + " exited with " + Constants.EXIT_CODE_UNKNOWN + ": "
              + sshResult);
    }
    if (mShutdown) {
      mLogger.warn("Shutting down host " + mHost.getName());
      return false;
    }
    boolean result;
    if (sshResult.getExitCode() != 0 || sshResult.getException() != null) {
      mLogger.debug(sshResult.getOutput());
      result = false;
      batchLogDir = Dirs.create(new File(mFailedTestLogDir, batch.getName()));
    } else {
      result = true;
      batchLogDir = Dirs.create(new File(mSuccessfulTestLogDir, batch.getName()));
    }
    String copyLogsCommand = dockerClient.getCopyTestLogsCommand(containerName, batchLogDir.getAbsolutePath());
    sw = Stopwatch.createStarted();
    mLogger.info("Copying logs for the " + batch + " with " + copyLogsCommand);
    sshResult = new SSHCommand(mSSHCommandExecutor, mPrivateKey, mHost.getUser(),
        mHost.getName(), containerInstanceId, copyLogsCommand, true).
        call();
    sw.stop();
    if (sshResult.getExitCode() != 0 || sshResult.getException() != null) {
      mLogger.error("Could not copy logs for batch [{}] on host {} using container instance {}. ElapsedTime(ms)={}",
          new Object[] { batch.getName(), mHost.toShortString(), containerInstanceId,
              sw.elapsed(TimeUnit.MILLISECONDS) });
      //TODO do we need to throw error?
      //throw new AbortContainerException("Could not stop container after test execution");
    } else {
      mLogger.info(
          "Completed copying logs for batch [{}] on host {} using container instance {}. ElapsedTime(ms)={}",
          new Object[] { batch.getName(), mHost.toShortString(), containerInstanceId,
              sw.elapsed(TimeUnit.MILLISECONDS) });
    }

    File logFile = new File(batchLogDir, String.format("%s.txt", batch.getName()));
    PrintWriter writer = new PrintWriter(logFile);
    writer.write(String.format("result = '%s'\n", sshResult.toString()));
    writer.write(String.format("output = '%s'\n", sshResult.getOutput()));
    if (sshResult.getException() != null) {
      sshResult.getException().printStackTrace(writer);
    }
    writer.close();

    //Copy log files from the container to Ptest server
    //TODO original code had String[] for localDirectories
    copyFromContainerHostToLocal(containerInstanceId, batchLogDir.getAbsolutePath(),
        mHost.getLocalDirectories()[0] + "/", fetchLogsForSuccessfulTests || !result);

    //TODO add code to shutdown the container and delete it
    String stopContainerCommand = dockerClient.getStopContainerCommand(containerName, true);
    sw = Stopwatch.createStarted();
    mLogger.info("Stopping container " + containerName + " with " + stopContainerCommand);
    sshResult = new SSHCommand(mSSHCommandExecutor, mPrivateKey, mHost.getUser(),
        mHost.getName(), containerInstanceId, stopContainerCommand, true).
        call();
    sw.stop();
    if (sshResult.getExitCode() != 0 || sshResult.getException() != null) {
      throw new AbortContainerException("Could not stop container after test execution");
    }
    return true;
  }

  public String getContainerName(int containerInstanceId) {
    return mHost.getName() + "-" + mTemplateDefaults.get("buildTag") + "-" + String
        .valueOf(containerInstanceId);
  }

  RSyncResult copyFromContainerHostToLocal(int containerInstanceId, String localFile, String remoteFile, boolean fetchAllLogs)
      throws SSHExecutionException, IOException {
    Map<String, String> templateVariables = Maps.newHashMap(mTemplateDefaults);
    //TODO do we need this here?
    //templateVariables.put("instanceName", drone.getInstanceName());
    //templateVariables.put("localDir", drone.getLocalDirectory());
    RSyncResult result = new RSyncCommand(mRSyncCommandExecutor, mPrivateKey, mHost.getUser(),
        mHost.getName(), containerInstanceId,
        Templates.getTemplateResult(localFile, templateVariables),
        Templates.getTemplateResult(remoteFile, templateVariables),
        fetchAllLogs ? RSyncCommand.Type.TO_LOCAL : RSyncCommand.Type.TO_LOCAL_NON_RECURSIVE).call();
    if(result.getException() != null || result.getExitCode() != Constants.EXIT_CODE_SUCCESS) {
      throw new SSHExecutionException(result);
    }
    totalElapsedTimeInRsync.getAndAdd(result.getElapsedTimeInMs());
    return result;
  }
}
