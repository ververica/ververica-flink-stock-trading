package com.ververica.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Stock {
    private String stockId;
    private String symbol;
    private String companyName;
    private double currentPrice;
    private long lastUpdated;
}
