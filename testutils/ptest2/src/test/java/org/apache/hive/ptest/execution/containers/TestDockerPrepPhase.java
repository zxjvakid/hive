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

import org.apache.commons.io.FileUtils;
import org.apache.hive.ptest.execution.AbstractTestPhase;

import org.apache.hive.ptest.execution.LocalCommand;
import org.apache.hive.ptest.execution.LocalCommand.CollectLogPolicy;
import org.apache.hive.ptest.execution.LocalCommandFactory;
import org.apache.hive.ptest.execution.ssh.NonZeroExitCodeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class TestDockerPrepPhase extends AbstractTestPhase {
  private DockerPrepPhase phase;
  private static File dummyPatchFile;
  private static final Logger logger = LoggerFactory.getLogger(TestDockerPrepPhase.class);

  @BeforeClass
  public static void beforeClass() throws Exception {
    dummyPatchFile = File.createTempFile("dummy", "patch");
    dummyPatchFile.deleteOnExit();
    Assert.assertTrue("Could not create dummy patch file " + dummyPatchFile.getAbsolutePath(),
        dummyPatchFile.exists());
  }

  @Before
  public void setup() throws Exception {
    initialize(getClass().getSimpleName());
    createHostExecutor();
    phase = new DockerPrepPhase(hostExecutors, localCommandFactory,
        templateDefaults, baseDir, dummyPatchFile, logger);
  }

  @After
  public void teardown() {
    phase = null;
    FileUtils.deleteQuietly(baseDir);
  }

  @Test
  public void testExecute() throws Exception {
    phase.execute();
    Assert.assertNotNull("Scratch directory needs to be set", phase.getLocalScratchDir());
    File dockerFile = new File(phase.getLocalScratchDir(), "Dockerfile");
    Assert.assertTrue("Docker file not found", dockerFile.exists());
  }
}
