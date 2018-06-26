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

import com.google.common.base.Splitter;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import org.apache.hive.ptest.execution.ssh.SSHCommand;
import org.apache.hive.ptest.execution.ssh.SSHCommandExecutor;
import org.apache.hive.ptest.execution.ssh.TestSSHCommandExecutor;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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

  private static class TestSSHCommandExecutor extends SSHCommandExecutor {
    private final List<String> mCommands;
    private final Map<String, Queue<Integer>> mFailures;
    private final AtomicInteger matchCount = new AtomicInteger(0);
    public TestSSHCommandExecutor(Logger logger) {
      super(logger);
      mCommands = Lists.newArrayList();
      mFailures = Maps.newHashMap();
    }
    public synchronized List<String> getCommands() {
      return mCommands;
    }
    public synchronized void putFailure(String command, Integer... exitCodes) {
      Queue<Integer> queue = mFailures.get(command);
      if(queue == null) {
        queue = new LinkedList<Integer>();
        mFailures.put(command, queue);
      } else {
        queue = mFailures.get(command);
      }
      for(Integer exitCode : exitCodes) {
        queue.add(exitCode);
      }
    }
    @Override
    public synchronized void execute(SSHCommand command) {
      mCommands.add(command.getCommand());
      command.setOutput("");
      Queue<Integer> queue = mFailures.get(command.getCommand());
      if(queue == null || queue.isEmpty()) {
        command.setExitCode(0);
      } else {
        matchCount.incrementAndGet();
        command.setExitCode(queue.remove());
      }
    }

    public int getMatchCount() {
      return matchCount.get();
    }
  }
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
  private static final String PRIVATE_KEY = "~/.ssh/id_rsa";
  private static final String USER = "vihang";
  private static final String HOST = "localhost";
  private static final int INSTANCE = 13;
  private static final String INSTANCE_NAME = HOST + "-" + USER + "-" + INSTANCE;
  private static final String REAL_BRANCH = "master";
  private static final String REAL_REPOSITORY = "https://github.com/apache/hive.git";
  private static final String REAL_REPOSITORY_NAME = "apache-hive";
  private static final String REAL_MAVEN_OPTS = "-Xmx2048m";
  private static final String DOCKER_EXEC_PATH = "/usr/local/bin/docker";
  private SSHCommandExecutor sshCommandExecutor;
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
    executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("DockerHostExecutor %d").build()));
    executionContext = mock(ExecutionContext.class);
    hostExecutorBuilder = mock(HostExecutorBuilder.class);
    //use real localCommandFactory
    localCommandFactory = new LocalCommandFactory(logger);
    sshCommandExecutor = new SSHCommandExecutor(logger);
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
        .put("containerLogDir", "/tmp/testlogs")
        .put("dockerExecPath", DOCKER_EXEC_PATH)
        .build();
    host = new Host(HOST, USER, new String[] { LOCAL_DIR }, 2);
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    URL url = TestPtestOnDockers.class.getResource("/DUMMY-002.patch");
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
    AtomicInteger batchIdCounter = new AtomicInteger(1);
    TestBatch qfileTestBatch = new QFileTestBatch(batchIdCounter, "test", "TestCliDriver", "qfile",
        Sets.newHashSet(Splitter.on(", ").split(
            "ppd_join3.q, auto_join23.q, list_bucket_dml_11.q, join10.q, udf_lower.q, "
                + "avro_type_evolution.q, constprog_dp.q, create_struct_table.q, "
                + "skewjoin_mapjoin9.q, check_constraint.q, hook_context_cs.q, "
                + "vector_parquet_nested_two_level_complex.q, exim_22_import_exist_authsuccess.q,"
                + "groupby1.q, create_func1.q, cbo_rp_udf_udaf.q, vector_decimal_aggregate.q,"
                + "create_skewed_table1.q, partition_wise_fileformat.q, union_ppr.q,"
                + "spark_combine_equivalent_work.q, stats_partial_size.q, join32.q,"
                + "list_bucket_dml_14.q, input34.q, udf_parse_url.q, "
                + "schema_evol_text_nonvec_part.q, enforce_constraint_notnull.q, "
                + "zero_rows_single_insert.q, ctas_char.q")),
        true, "itests/qtest");

    /*TestBatch unitTestBatch = new UnitTestBatch(new AtomicInteger(1), "test", Lists.newArrayList(
        Splitter.on(", ").split("TestCommands, TestUserHS2ConnectionFileParser, TestBufferedRows, "
            + "TestBeeLineOpts, TestHiveCli, TestClientCommandHookFactory, "
            + "TestBeeLineExceptionHandling, TestBeelineArgParsing, TestIncrementalRows, "
            + "TestShutdownHook, TestBeeLineHistory, TestHiveSchemaTool, TestTableOutputFormat")),
        "beeline", true);*/
    TestBatch unitTestBatch = new UnitTestBatch(batchIdCounter, "test", Lists.newArrayList(
        Splitter.on(", ").split("TestCommands, TestUserHS2ConnectionFileParser, TestBufferedRows, "
            + "TestBeeLineOpts")),
        "beeline", true);

    TestBatch failingQueryTestBatch =
        new QFileTestBatch(batchIdCounter, "test", "TestCliDriver", "qfile",
            Sets.newHashSet(Splitter.on(", ").split("dummy_failing_test.q")), true, "itests/qtest");

    TestBatch failingUnitTestBatch =
        new UnitTestBatch(batchIdCounter, "test", Lists.newArrayList("TestFakeFailure"), "service",
            true);
    testBatches.add(qfileTestBatch);
    testBatches.add(unitTestBatch);
    testBatches.add(failingQueryTestBatch);
    testBatches.add(failingUnitTestBatch);
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
  public void testPrepPhase() throws Throwable {
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
