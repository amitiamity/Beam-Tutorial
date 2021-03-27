package com.amiown.beam.tutorial;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

/**
 * word count aggregation on input streams
 */
public class WordCount {

    public static void main(String... s) {

        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from("src/main/resources/source/words.txt"))
                /*FlatMapElements is used when one input element results into multiple elements
                    For example, here 1 line will be split into multiple words (elements)
                    TypeDescriptors defines the output type of the element
                */
                .apply("ExtractWords", FlatMapElements.into(TypeDescriptors.strings())
                        // split the line into multiple words using spaces
                        .via((String line) -> Arrays.asList(line.toLowerCase().split(" "))))
                .apply("CountWords", Count.perElement())
                .apply("FormatResult", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) -> wordCount.getKey() + " : " + wordCount.getValue()))
                .apply(TextIO.write().to("src/main/resources/sink/wordsCountResult.txt"));

        pipeline.run().waitUntilFinish();
    }
}
