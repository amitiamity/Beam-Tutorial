package com.amiown.beam.tutorial;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class TotalScoreComputation {
    private static final String CSV_HEADER = "ID,Name,Physics,Chemistry,Math,English,Biology,History";

    public interface TotalScoreComputationOptions extends  PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("src/main/resources/source/student_scores.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
      //  @Default.String("src/main/resources/source/sink/student_total_scores.csv")
        @Validation.Required
        String getOutputFile();

        void setOutputFile(String value);
    }

    public static void main(String... s) {
        TotalScoreComputationOptions options = PipelineOptionsFactory.fromArgs(s)
                .withValidation().as(TotalScoreComputationOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeTotalScoreFn()))
                .apply(ParDo.of(new ConvertToStringFn()))
                .apply(TextIO.write().to(options.getOutputFile())
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
