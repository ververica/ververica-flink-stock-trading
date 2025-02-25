package com.ververica.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MarketEvent {
    private String eventId;
    private String stockSymbol;
    private String eventType;
    private double impactOnPrice;
    private long eventTime;
}
