package com.amiown.beam.tutorial;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class WordCount2 {


    /**
     * custom pipelines options
     */
    public interface CustomPipelineOptions extends PipelineOptions {

        @Description("Path of the input file")
        @Default.String("src/main/resources/source/words.txt")
        String getInputFile();

        void setInputFile(String value);

        @Description(("Path of the output file"))
        @Default.String("src/main/resources/sink/words2_result.txt")
        @Validation.Required
        String getOutputFile();

        void setOutputFile(String value);
    }

    public static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
                Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");


        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<String> outputReceiver) {
            lineLenDist.update(line.length());
            if (line.trim().isEmpty()) {
                emptyLines.inc();
            }

            //split the line into words
            String[] words = line.split(" ");

            for (String word : words) {
                if (StringUtils.isNotEmpty(word)) {
                    outputReceiver.output(word);
                }
            }


        }
    }

    public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
            //extract the words from the lines
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

            //count of each word and return it
            return words.apply(Count.perElement());
        }
    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {

        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    static void runWordCountFn(CustomPipelineOptions pipelineOptions) {
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        pipeline.apply("ReadLines", TextIO.read().from(pipelineOptions.getInputFile()))
                //count words
                .apply("CountWords", new CountWords())
                .apply("FormatOutput", MapElements
                        .via(new FormatAsTextFn()))
                .apply("WriteFile", TextIO.write().to(pipelineOptions.getOutputFile()));


        pipeline.run().waitUntilFinish();
    }

    public static void main(String... s) {
        CustomPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(s)
                .withValidation().as(CustomPipelineOptions.class);
        runWordCountFn(pipelineOptions);
    }
}
