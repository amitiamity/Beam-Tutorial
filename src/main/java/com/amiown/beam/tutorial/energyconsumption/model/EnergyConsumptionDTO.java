package com.amiown.beam.tutorial.energyconsumption.model;

import lombok.Getter;
import lombok.Setter;
import org.joda.time.Instant;

import java.io.Serializable;

@Getter
@Setter
public class EnergyConsumptionDTO implements Serializable {

    private static final String[] headers = {"Datetime", "AEP_MW"};

    private Instant dateTime;
    private Double energyConsumption;

    public String asCSVRow(String delimiter) {
        return String.join(delimiter, this.dateTime.toString(), this.energyConsumption.toString());
    }

    public static String getCSVHeader() {
        return String.join(",", "Datetime", "MW");
    }


}
