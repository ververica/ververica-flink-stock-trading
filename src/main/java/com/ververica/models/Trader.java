package com.ververica.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Trader {
    private String traderId;
    private String name;
    private String tradingFirm;
    private double balance;
    private long lastUpdated;
}
