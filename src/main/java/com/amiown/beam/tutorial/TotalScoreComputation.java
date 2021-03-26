package com.amiown.beam.tutorial;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class TotalScoreComputation {
    private static final String CSV_HEADER = "ID,Name,Physics,Chemistry,Math,English,Biology,History";

    public static void main(String... s) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from("src/main/resources/source/student_scores.csv"))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeTotalScoreFn()))
                .apply(ParDo.of(new ConvertToStringFn()))
                .apply(TextIO.write().to("src/main/resources/sink/student_total_scores.csv")
                //add header in output file
                        .withHeader("Name,Total")
                //make sure to write into a single file
                        .withNumShards(1)
                );


        pipeline.run().waitUntilFinish();
    }

    private static class FilterHeaderFn extends DoFn<String, String> {

        private String headerStr;

        public FilterHeaderFn(String headerStr) {
            this.headerStr = headerStr;
        }

        /**
         * skip the line which has the header line
         *
         * @param context
         */
        @ProcessElement
        public void processElement(ProcessContext context) {
            final String text = context.element();

            if (!StringUtils.isEmpty(text) && !text.equals(headerStr)) {
                context.output(text);
            }
        }
    }

    public static class ComputeTotalScoreFn extends DoFn<String, KV<String, Integer>> {

        private String score;

        @ProcessElement
        public void processElement(ProcessContext context) {
            String[] data = context.element().split(",");
            String name = data[1];

            Integer totalScore = Integer.parseInt(data[0]) + Integer.parseInt(data[2]) +
                    Integer.parseInt(data[3]) + Integer.parseInt(data[4]) + Integer.parseInt(data[5]) +
                    Integer.parseInt(data[6]) + Integer.parseInt(data[7]);

            context.output(KV.of(name, totalScore));
        }
    }

    public static class ConvertToStringFn extends DoFn<KV<String, Integer>, String> {

        @ProcessElement
        public void processElement(ProcessContext context) {
            context.output(context.element().getKey() + "," + context.element().getValue());
        }
    }
}
