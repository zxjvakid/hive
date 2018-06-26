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

import org.apache.hive.ptest.execution.ContainerClient;
import org.apache.hive.ptest.execution.ContainerClientFactory.ContainerClientContext;
import org.apache.hive.ptest.execution.Templates;
import org.apache.hive.ptest.execution.conf.TestBatch;
import org.slf4j.Logger;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

//TODO this implementation to actually run the commands instead of returning string commands
public class DockerClient implements ContainerClient {

  private static final String USER = "ptestuser";
  private static final String HOME_DIR = File.separatorChar + "home" + File.separatorChar + USER;
  private static final String DOCKER_SCRATCH_DIR = HOME_DIR + File.separatorChar + "scratch" + File.separatorChar;
  private static Logger logger;
  private final ContainerClientContext context;
  private final String DOCKER_EXEC;
  public DockerClient(ContainerClientContext context) {
    logger = context.getLogger();
    this.context = context;
    String dockerPath = context.getTemplateDefaults().get("dockerExecPath");
    DOCKER_EXEC = dockerPath == null ? "/usr/bin/docker" : dockerPath;
  }

  @Override
  public void defineImage(String dir) throws Exception {
    final String label = context.getTemplateDefaults().get("buildTag");
    if (label == null) {
      throw new Exception("buildTag not found");
    }
    File dockerfile = new File(dir, "Dockerfile");
    File copyTestLogs = new File(dir, "copy-test-logs.sh");
    logger.info("Writing {} from template", dockerfile);
    Map<String, String> templateDefaults = context.getTemplateDefaults();
    Templates.writeTemplateResult("dockerfile-template.vm", dockerfile, templateDefaults);
    //generate the script to copy test logs
    Templates.writeTemplateResult("copy-test-logs.vm", copyTestLogs, templateDefaults);
  }

  @Override
  public String getBuildCommand(String dir, long toWait, TimeUnit unit) throws Exception {
    //TODO timeout not used currently
    long seconds = TimeUnit.SECONDS.convert(toWait, unit);
    logger.info("Building image");
    String dockerBuildCommand =
        new StringBuilder(DOCKER_EXEC + " build")
            .append(" --memory " + "2g")
            .append(" --memory-swap " + "-1")
            .append(" --tag " + imageName())
            .append(" --build-arg ")
            .append(" workingDir=$workingDir")
            .append(" --build-arg ")
            .append(" patchFilePath=$workingDir/scratch/build.patch")
            //TODO confirm if we need antEnvOpts
            //.append(" antEnvOpts=$antEnvOpts")
            .append(" --build-arg ")
            .append(" mavenEnvOpts=$mavenEnvOpts")
            .append(" --build-arg ")
            .append(" repository=$repository")
            .append(" --build-arg ")
            .append(" branch=$branch")
            .append(" " + dir)
            .toString();
    return dockerBuildCommand;
  }

  @Override
  public String getRunContainerCommand(String containerName, TestBatch batch) {
    return new StringBuilder(DOCKER_EXEC + " run")
        .append(" --memory " + "2G")
        .append(" --name " + containerName)
        //.append(" -d")
        .append(" -t " + imageName())
        .append(" /bin/bash")
        .append(" -c")
        .append(" \"cd " + batch.getTestModuleRelativeDir() + ";")
        .append(" /usr/bin/mvn")
        .append(" -Dsurefire.timeout=2400")
        .append(" -B test")
        .append(" " + batch.getTestArguments())
        .append(
            " 1>" + File.separatorChar + "/tmp" + File.separatorChar + "maven.txt")
        .append(" 2>&1 ;")
        .append(
            " /bin/bash /home/ptestuser/scratch/copy-test-logs.sh 1>" + File.separatorChar + "tmp"
                + File.separatorChar + "copy-test-logs.txt 2>&1; \"")
        .toString();
  }

  @Override
  public String getCopyTestLogsCommand(String containerName, String dir) {
    String containerLogDir = context.getTemplateDefaults().get("containerLogDir");
    return new StringBuilder(DOCKER_EXEC + " cp")
        .append(" " + containerName + ":" + containerLogDir)
        .append(" " + dir)
        .toString();
  }

  @Override
  public String getStopContainerCommand(String containerName, boolean forceRemove) {
    StringBuilder ret = new StringBuilder(DOCKER_EXEC + " stop " + containerName);
    if (forceRemove) {
      ret.append("; " + DOCKER_EXEC + " rm " + containerName);
    }
    return ret.toString();
  }

  private String imageName() {
    return context.getTemplateDefaults().get("buildTag");
  }
}
