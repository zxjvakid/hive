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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hive.ptest.execution.LocalCommand.CollectLogPolicy;
import org.apache.hive.ptest.execution.conf.Host;
import org.apache.hive.ptest.execution.conf.QFileTestBatch;
import org.apache.hive.ptest.execution.conf.TestBatch;
import org.apache.hive.ptest.execution.conf.UnitTestBatch;
import org.apache.hive.ptest.execution.containers.DockerExecutionPhase;
import org.apache.hive.ptest.execution.containers.DockerHostExecutor;
import org.apache.hive.ptest.execution.containers.DockerPrepPhase;
import org.apache.hive.ptest.execution.containers.TestDockerPrepPhase;
import org.apache.hive.ptest.execution.context.ExecutionContext;
import org.apache.hive.ptest.execution.ssh.NonZeroExitCodeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * This test assumes that the host where this test is being run has docker installed
 */
public class TestPtestOnDockers {
  //TODO add logic to ignore this test if docker is not found on the machine

  private DockerPrepPhase prepPhase;
  private DockerExecutionPhase execPhase;
  private static File dummyPatchFile;
  private static final Logger logger = LoggerFactory.getLogger(TestPtestOnDockers.class);

  private File baseDir;
  private File scratchDir;
  private File logDir;
  private File succeededLogDir;
  private File failedLogDir;
  private ListeningExecutorService executor;
  private LocalCommandFactory localCommandFactory;
  private LocalCommand localCommand;
  private ImmutableMap<String, String> templateDefaults;
  private ImmutableList<HostExecutor> hostExecutors;
  private HostExecutor hostExecutor;
  private ExecutionContext executionContext;
  private HostExecutorBuilder hostExecutorBuilder;
  private Host host;

  private static final String LOCAL_DIR = "/some/local/dir";
  private static final String PRIVATE_KEY = "some.private.key";
  private static final String USER = "someuser";
  private static final String HOST = "somehost";
  private static final int INSTANCE = 13;
  private static final String INSTANCE_NAME = HOST + "-" + USER + "-" + INSTANCE;
  private static final String REAL_BRANCH = "master";
  private static final String REAL_REPOSITORY = "https://github.com/apache/hive.git";
  private static final String REAL_REPOSITORY_NAME = "apache-hive";
  private static final String REAL_MAVEN_OPTS = "-Xmx2048m";
  private MockSSHCommandExecutor sshCommandExecutor;
  private MockRSyncCommandExecutor rsyncCommandExecutor;
  private static final String BUILD_TAG = "docker-ptest-tag";
  private final Set<String> executedTests = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  private final Set<String> failedTests = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  private List<TestBatch> testBatches;

  public void initialize(String name) throws Exception {
    baseDir = AbstractTestPhase.createBaseDir(name);
    logDir = Dirs.create(new File(baseDir, "logs"));
    scratchDir = Dirs.create(new File(baseDir, "scratch"));
    succeededLogDir = Dirs.create(new File(logDir, "succeeded"));
    failedLogDir = Dirs.create(new File(logDir, "failed"));
    executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
    executionContext = mock(ExecutionContext.class);
    hostExecutorBuilder = mock(HostExecutorBuilder.class);
    //use real localCommandFactory
    localCommandFactory = new LocalCommandFactory(logger);
    sshCommandExecutor = spy(new MockSSHCommandExecutor(logger));
    rsyncCommandExecutor = spy(new MockRSyncCommandExecutor(logger));
    templateDefaults = ImmutableMap.<String, String>builder()
        .put("localDir", LOCAL_DIR)
        .put("buildTag", BUILD_TAG)
        //use baseDir as working directory
        .put("workingDir", baseDir.getAbsolutePath())
        .put("instanceName", INSTANCE_NAME)
        .put("branch", REAL_BRANCH)
        .put("logDir", logDir.getAbsolutePath())
        .put("repository", REAL_REPOSITORY)
        .put("repositoryName", REAL_REPOSITORY_NAME)
        .put("mavenEnvOpts", REAL_MAVEN_OPTS)
        .build();
    host = new Host(HOST, USER, new String[] { LOCAL_DIR }, 2);
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    URL url = TestPtestOnDockers.class.getResource("/DUMMY-001.patch");
    dummyPatchFile = new File(url.getFile());
    Assert.assertTrue("Could not find dummy patch file " + dummyPatchFile.getAbsolutePath(),
        dummyPatchFile.exists());
  }

  @Before
  public void setup() throws Exception {
    initialize(getClass().getSimpleName());
    createHostExecutor();
    prepPhase = new DockerPrepPhase(hostExecutors, localCommandFactory,
        templateDefaults, baseDir, dummyPatchFile, logger);
    createTestBatches();
    execPhase = new DockerExecutionPhase(hostExecutors, executionContext,
        hostExecutorBuilder, localCommandFactory,
        templateDefaults, succeededLogDir, failedLogDir,
        Suppliers.ofInstance(testBatches), executedTests,
        failedTests, logger);
  }

  private void createTestBatches() throws Exception {
    testBatches = new ArrayList<>();
    TestBatch qfileTestBatch = new QFileTestBatch(new AtomicInteger(1), "", "TestCliDriver", "",
        Sets.newHashSet("insert0.q"), true, "itests/qtest");
    testBatches.add(qfileTestBatch);
  }

  private void createHostExecutor() throws Exception {
    hostExecutor = new DockerHostExecutor(host, PRIVATE_KEY, executor, sshCommandExecutor,
        rsyncCommandExecutor, templateDefaults, scratchDir, succeededLogDir, failedLogDir, 1, true,
        logger);
    hostExecutors = ImmutableList.of(hostExecutor);
  }

  @After
  public void teardown() {
    prepPhase = null;
    //FileUtils.deleteQuietly(baseDir);
  }

  /**
   * This test requires docker to be installed to test on local machine
   * @throws Exception
   */
  @Test
  public void testDockerFile() throws Throwable {
    prepPhase.execute();
    Assert.assertNotNull("Scratch directory needs to be set", prepPhase.getLocalScratchDir());
    File dockerFile = new File(prepPhase.getLocalScratchDir(), "Dockerfile");
    Assert.assertTrue("Docker file not found", dockerFile.exists());
  }

  @Test
  public void testDockerExecutionPhase() throws Throwable {
    execPhase.execute();
  }
}
