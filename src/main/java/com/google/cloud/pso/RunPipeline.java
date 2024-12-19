/*
 *  Copyright 2024 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.google.cloud.pso;

import com.google.cloud.pso.options.TaxiSessionsOptions;
import com.google.cloud.pso.pipelines.TaxiSessionsPipeline;
import java.time.Instant;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class RunPipeline {

  private static String getJobName(String pipelineToRun) {
    String nowStr = String.valueOf(Instant.now().getEpochSecond());
    return pipelineToRun + "-pipeline-" + nowStr;
  }

  public static void main(String[] args) {
    TaxiSessionsOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TaxiSessionsOptions.class);

    String pipelineToRun = options.getPipeline();

    Pipeline p = null;
    switch (pipelineToRun) {
      case "taxi-sessions":
        p = TaxiSessionsPipeline.createPipeline(options);
        break;
    }

    if (p != null) {
      String jobName = options.getJobName();
      // Set name only if not empty.
      // If jobName is already set, it might be because we are updating the job.
      if (jobName == null || jobName.isEmpty()) jobName = getJobName(pipelineToRun);
      p.getOptions().setJobName(jobName);
      p.run().waitUntilFinish();
    } else {
      System.out.println("Unrecognized pipeline type " + pipelineToRun);
      System.exit(1);
    }
  }
}
