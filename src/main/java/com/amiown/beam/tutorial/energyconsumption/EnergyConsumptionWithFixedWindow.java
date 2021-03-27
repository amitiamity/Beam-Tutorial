package com.amiown.beam.tutorial.energyconsumption;

import com.amiown.beam.tutorial.energyconsumption.model.EnergyConsumptionDTO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.io.StringReader;

public class EnergyConsumptionWithFixedWindow {


    public static void main(String... s) {

        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<EnergyConsumptionDTO> energyConsumptionDTOPCollection = pipeline.apply("ReadEnergyConsumption",
                TextIO.read().from("src/main/resources/source/AEP_hourly.csv"))
                .apply("ParseEnergyData", ParDo.of(new ParseEnergyDataFn()))
                //to extract embedded event time stamp to associate with element in the window
                .apply("TimeStamps", WithTimestamps.of(EnergyConsumptionDTO::getDateTime));

        //apply window interval
        energyConsumptionDTOPCollection.apply("Window", Window.into(FixedWindows.of(Duration.standardDays(1))))
                .apply("ToStrings", MapElements.into(TypeDescriptors.strings())
                        .via(us -> us.asCSVRow(",")))
                //to write result
                .apply("WriteToFile", TextIO.write().to("src/main/resources/sink/AEP_hourly_Result").withSuffix(".csv")
                        .withHeader(EnergyConsumptionDTO.getCSVHeader())
                        .withNumShards(1)
                        .withWindowedWrites());

        pipeline.run().waitUntilFinish();


    }


    public static class ParseEnergyDataFn extends DoFn<String, EnergyConsumptionDTO> {

        private static final String[] fileHeaderMappings = {"Datetime", "AEP_MW"};

        @ProcessElement
        public void processElement(ProcessContext processContext) throws IOException {

            final CSVParser csvParser = new CSVParser(new StringReader(
                    processContext.element()),
                    CSVFormat.DEFAULT
                            .withDelimiter(',')
                            .withHeader(fileHeaderMappings));
            CSVRecord csvRecord = csvParser.getRecords().get(0);
            if (csvRecord.get("Datetime").contains("Datetime")) {
                return;
            }

            DateTimeZone timeZone = DateTimeZone.forID("Asia/Kolkata");

            DateTime dateTime = LocalDateTime.parse(csvRecord.get("Datetime").trim(),
                    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).toDateTime(timeZone);

            EnergyConsumptionDTO energyConsumptionDTO = new EnergyConsumptionDTO();
            energyConsumptionDTO.setDateTime(dateTime.toInstant());
            energyConsumptionDTO.setEnergyConsumption(Double.valueOf(csvRecord.get("AEP_MW")));

            processContext.output(energyConsumptionDTO);
        }
    }
}
