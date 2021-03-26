package com.amiown.beam.tutorial;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;

import java.util.Arrays;

/**
 * filter the stream of in-memory data based on a threshold value
 */
public class Filter {

    public static void main(String... s) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(Create.of(Arrays.asList(1.15, 2.0, 3.12, 4.23, 5.86)))
                .apply(MapElements.via(new SimpleFunction<Double, Double>() {
                    @Override
                    public Double apply(Double input) {
                        System.out.println("-Pre-Filtered:" + input);
                        return input;
                    }
                }))
                .apply(ParDo.of(new FilterThresholdFn(3.4)))
                .apply(MapElements.via(new SimpleFunction<Double, Void>() {
                    @Override
                    public Void apply(Double input) {
                        System.out.println("-Post-Filtered: " + input);
                        return null;
                    }
                }));
        pipeline.run();
    }

    public static class FilterThresholdFn extends DoFn<Double, Double> {
        private Double threshold;

        public FilterThresholdFn(Double threshold) {
            this.threshold = threshold;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element() > threshold) {
                c.output(c.element());
            }
        }
    }
}
