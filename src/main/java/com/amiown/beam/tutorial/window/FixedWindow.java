package com.amiown.beam.tutorial.window;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public class FixedWindow {

    public static void main(String... s) {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        //artificial data sample for car make time
        PCollection<String> carMakesTime = pipeline.apply(Create.timestamped(

                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:05").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:06").toInstant()),
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:07").toInstant()),
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:08").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:11").toInstant()),
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:12").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:13").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:14").toInstant()),
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:16").toInstant()),
                TimestampedValue.of("Toyota", new DateTime("2020-12-12T20:30:17").toInstant())
        ));

        //defining the window interval for this data processing
        PCollection<String> windowMakersTimes = carMakesTime
                .apply("Window", Window.into((FixedWindows.of(Duration.standardSeconds(5)))));

        //Counting the number of elements created in each window
        PCollection<KV<String, Long>> output = windowMakersTimes
                .apply(Count.perElement());

        //printing the key value pair on the console
        output.apply(ParDo.of(new DoFn<KV<String, Long>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext processContext, BoundedWindow boundedWindow) {
                System.out.println(String.format("%s: %s %s", boundedWindow.maxTimestamp(),
                        processContext.element().getKey(), processContext.element().getValue()));
            }
        }));

        pipeline.run().waitUntilFinish();

    }
}
