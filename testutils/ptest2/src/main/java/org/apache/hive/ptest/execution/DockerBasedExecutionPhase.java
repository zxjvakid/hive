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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import org.apache.hive.ptest.execution.conf.TestBatch;
import org.apache.hive.ptest.execution.context.ExecutionContext;
import org.apache.hive.ptest.execution.ssh.RemoteCommandResult;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class DockerBasedExecutionPhase extends ExecutionPhase {

  public DockerBasedExecutionPhase(List<HostExecutor> hostExecutors,
      ExecutionContext executionContext, HostExecutorBuilder hostExecutorBuilder,
      LocalCommandFactory localCommandFactory, ImmutableMap<String, String> templateDefaults,
      File succeededLogDir, File failedLogDir, Supplier<List<TestBatch>> testBatchSupplier,
      Set<String> executedTests, Set<String> failedTests, Logger logger) throws IOException {
    super(hostExecutors, executionContext, hostExecutorBuilder, localCommandFactory,
        templateDefaults, succeededLogDir, failedLogDir, testBatchSupplier, executedTests,
        failedTests, logger);
  }

  @Override
  public void execute() throws Throwable {
    long start = System.currentTimeMillis();
    /*List<TestBatch> testBatches = Lists.newArrayList();
    for(TestBatch batch : testBatchSupplier.get()) {
      testBatches.add(batch);
      if(batch.isParallel()) {
        parallelWorkQueue.add(batch);
      } else {
        isolatedWorkQueue.add(batch);
      }
    }
    logger.info("ParallelWorkQueueSize={}, IsolatedWorkQueueSize={}", parallelWorkQueue.size(),
        isolatedWorkQueue.size());
    if (logger.isDebugEnabled()) {
      for (TestBatch testBatch : parallelWorkQueue) {
        logger.debug("PBatch: {}", testBatch);
      }
      for (TestBatch testBatch : isolatedWorkQueue) {
        logger.debug("IBatch: {}", testBatch);
      }
    }*/
    try {
      int expectedNumHosts = hostExecutors.size();
      initalizeHosts();
      resetPerfMetrics();
      /*do {
        //replaceBadHosts(expectedNumHosts);
        List<ListenableFuture<Void>> results = Lists.newArrayList();
        for(HostExecutor hostExecutor : ImmutableList.copyOf(hostExecutors)) {
          results.add(hostExecutor.submitTests(parallelWorkQueue, isolatedWorkQueue, failedTestResults));
        }
        Futures.allAsList(results).get();
      } while(!(parallelWorkQueue.isEmpty() && isolatedWorkQueue.isEmpty()));
      for(TestBatch batch : testBatches) {
        File batchLogDir;
        if(failedTestResults.contains(batch)) {
          batchLogDir = new File(failedLogDir, batch.getName());
        } else {
          batchLogDir = new File(succeededLogDir, batch.getName());
        }
        JUnitReportParser parser = new JUnitReportParser(logger, batchLogDir);
        executedTests.addAll(parser.getAllExecutedTests());
        for (String failedTest : parser.getAllFailedTests()) {
          failedTests.add(failedTest + " (batchId=" + batch.getBatchId() + ")");
        }

        // if the TEST*.xml was not generated or was corrupt, let someone know
        if (parser.getTestClassesWithReportAvailable().size() < batch.getTestClasses().size()) {
          Set<String> expTestClasses = new HashSet<>(batch.getTestClasses());
          expTestClasses.removeAll(parser.getTestClassesWithReportAvailable());
          for (String testClass : expTestClasses) {
            StringBuilder messageBuilder = new StringBuilder();
            messageBuilder.append(testClass).append(" - did not produce a TEST-*.xml file (likely timed out)")
                .append(" (batchId=").append(batch.getBatchId()).append(")");
            if (batch instanceof QFileTestBatch) {
              Collection<String> tests = ((QFileTestBatch)batch).getTests();
              if (tests.size() != 0) {
                messageBuilder.append("\n\t[");
                messageBuilder.append(Joiner.on(",").join(tests));
                messageBuilder.append("]");
              }
            }
            failedTests.add(messageBuilder.toString());
          }
        }
      }*/
    } finally {
      long elapsed = System.currentTimeMillis() - start;
      //addAggregatePerfMetrics();
      logger.info("PERF: exec phase " + TimeUnit.MINUTES.convert(elapsed, TimeUnit.MILLISECONDS) + " minutes");
    }
  }
}
