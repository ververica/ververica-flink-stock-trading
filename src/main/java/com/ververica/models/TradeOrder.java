package com.ververica.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TradeOrder {
    private String orderId;
    private String traderId;
    private String symbol;
    private int quantity;
    private double price;
    private String orderType;
    private long orderTime;
}
