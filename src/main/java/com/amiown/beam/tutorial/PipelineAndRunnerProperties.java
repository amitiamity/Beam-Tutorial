package com.amiown.beam.tutorial;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class PipelineAndRunnerProperties {

    /**
     * just to show the default properties of the apache beam
     * @param s
     */
    public static void main(String...s) {
        PipelineOptions options = PipelineOptionsFactory.create();

        System.out.println("Runner " + options.getRunner().getName());
        System.out.println("JobName " + options.getJobName());
        System.out.println("OptionsId " + options.getOptionsId());
        System.out.println("StableUniqueName " + options.getStableUniqueNames());
        System.out.println("TempLocation " + options.getTempLocation());
        System.out.println("UserAgent " + options.getUserAgent());

    }

}
