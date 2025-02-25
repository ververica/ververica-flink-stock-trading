package com.ververica.datagen;

import com.github.javafaker.Faker;
import com.ververica.models.*;

import java.util.List;
import java.util.Random;
import java.util.UUID;

public class StockMarketDataGenerator {
    private static final Faker faker = new Faker();
    private static final Random random = new Random();
    private static final String[] orderTypes = {"BUY", "SELL"};
    private static final StockEvent[] eventTypes = {
            new StockEvent("Earnings Report (Beat)", "Positive"),
            new StockEvent("Earnings Report (Miss)", "Negative"),
            new StockEvent("Product Launch","Positive"),
            new StockEvent("Product Recall","Negative"),
            new StockEvent("Stock Buyback","Positive"),
            new StockEvent("CEO Resignation","Negative"),
            new StockEvent("New CEO Appointment","Positive"),
            new StockEvent("Bankruptcy Filing","Negative"),
            new StockEvent("Lawsuit Against Company","Negative"),
            new StockEvent("Interest Rate Hike","Negative"),
            new StockEvent("Interest Rate Cut","Positive"),
            new StockEvent("Inflation Report (Higher)", "Negative"),
            new StockEvent("Inflation Report (Lower)","Positive"),
            new StockEvent("Unemployment Rate Increase","Negative"),
            new StockEvent("SEC Investigation","Negative"),
            new StockEvent("New Tax Regulations","Negative"),
            new StockEvent("Antitrust Ruling","Negative"),
            new StockEvent("Government Contract Awarded","Positive"),
            new StockEvent("Oil Price Surge","Negative"),
            new StockEvent("Oil Price Drop","Positive"),
            new StockEvent("Geopolitical Conflict","Negative"),
            new StockEvent("Trade Tariffs Introduced","Negative"),
            new StockEvent("Trade Tariffs Removed","Positive"),
            new StockEvent("Cybersecurity Breach","Negative"),
            new StockEvent("AI Breakthrough","Positive"),
            new StockEvent("Major Cloud Outage","Negative"),
            new StockEvent("Bitcoin Price Surge","Positive"),
            new StockEvent("Bitcoin Crash","Negative"),
            new StockEvent("Earthquake in Major Region","Negative"),
            new StockEvent("Viral Social Media Trend", "Positive")
    };

    public static Trader generateTrader(int id) {
        return new Trader(
                "FxTrader" + id,
                faker.name().fullName(),
                faker.company().name(),
                faker.number().randomDouble(2, 10000, 500000),
                System.currentTimeMillis()
        );
    }

    public static Stock generateStock(int id) {
        return new Stock(
                "IDSTCK" + id,
                faker.stock().nsdqSymbol(),
                faker.company().name(),
                faker.number().randomDouble(2, 5, 500),
                System.currentTimeMillis()- random.nextInt(100000)
        );
    }

    public static TradeOrder generateTradeOrder(List<Trader> traders, List<Stock> stocks) {
        Stock stock = stocks.get(random.nextInt(stocks.size()));
        Trader trader = traders.get(random.nextInt(traders.size()));

        return new TradeOrder(
                UUID.randomUUID().toString(),
                trader.getTraderId(),
                stock.getSymbol(),
                faker.number().numberBetween(1, 100),
                stock.getCurrentPrice(),
                orderTypes[random.nextInt(orderTypes.length)],
                System.currentTimeMillis()- random.nextInt(100000)
        );
    }


    public static MarketEvent generateMarketEvent(List<Stock> stocks) {
        Stock stock = stocks.get(random.nextInt(stocks.size()));
        StockEvent eventType = eventTypes[random.nextInt(eventTypes.length)];

        double impactOnPrice;

        if (eventType.getStockImpact().equals("Positive")) {
            impactOnPrice = faker.number().randomDouble(2, 0, 10);
        } else {
            impactOnPrice = faker.number().randomDouble(2, -10, 0);
        }

        return new MarketEvent(
                UUID.randomUUID().toString(),
                stock.getSymbol(),
                eventType.getEventType(),
                impactOnPrice, // // Impact on price
                System.currentTimeMillis()- random.nextInt(100000)
        );
    }
}
