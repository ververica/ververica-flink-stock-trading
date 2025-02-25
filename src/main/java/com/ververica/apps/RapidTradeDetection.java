package com.ververica.apps;

import com.ververica.models.EnrichedTradeOrder;
import com.ververica.models.TradeOrder;
import com.ververica.models.Trader;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

public class RapidTradeDetection {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Sample Trade Order Stream
        DataStream<TradeOrder> tradeStream = env.fromElements(
                        new TradeOrder("order_1", "trader_1", "AAPL", 100, 150.0, "BUY", System.currentTimeMillis()),
                        new TradeOrder("order_2", "trader_1", "GOOGL", 50, 2800.0, "SELL", System.currentTimeMillis() + 1000),
                        new TradeOrder("order_3", "trader_1", "MSFT", 75, 330.0, "BUY", System.currentTimeMillis() + 2000),
                        new TradeOrder("order_4", "trader_2", "TSLA", 30, 700.0, "BUY", System.currentTimeMillis() + 5000),
                        new TradeOrder("order_5", "trader_1", "NFLX", 40, 600.0, "BUY", System.currentTimeMillis() + 3000)
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeOrder>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<TradeOrder>) (trade, timestamp) -> trade.getOrderTime()));

        // Sample Trader Stream
        DataStream<Trader> traderStream = env.fromElements(
                new Trader("trader_1", "Ben Johnson", "ABC Trading", 50000.0, System.currentTimeMillis()),
                new Trader("trader_2", "Kate Smith", "XYZ Capital", 75000.0, System.currentTimeMillis())
        );

        // Keyed Stream of TradeOrders
        KeyedStream<TradeOrder, String> keyedTradeStream = tradeStream.keyBy(TradeOrder::getTraderId);

        // Keyed Stream of Traders (for state-based enrichment)
        KeyedStream<Trader, String> keyedTraderStream = traderStream.keyBy(Trader::getTraderId);

        // Enrich TradeOrder with Trader Details
        DataStream<EnrichedTradeOrder> enrichedTradeStream = keyedTradeStream.connect(keyedTraderStream)
                .process(new KeyedCoProcessFunction<String, TradeOrder, Trader, EnrichedTradeOrder>() {
                    private ValueState<Trader> traderState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        this.traderState = getRuntimeContext().getState(new ValueStateDescriptor<>("traderState", Trader.class));
                    }

                    @Override
                    public void processElement1(TradeOrder trade, Context ctx, Collector<EnrichedTradeOrder> out) throws Exception {
                        Trader trader = traderState.value();
                        if (trader != null) {
                            out.collect(new EnrichedTradeOrder(trade, trader));
                        }
                    }

                    @Override
                    public void processElement2(Trader trader, Context ctx, Collector<EnrichedTradeOrder> out) throws Exception {
                        traderState.update(trader);
                    }
                });

        // Define CEP Pattern: Detect rapid trading
        Pattern<EnrichedTradeOrder, ?> rapidTradePattern = Pattern.<EnrichedTradeOrder>begin("rapidTrades")
                .times(3)
                .within(Time.seconds(5));

        // Apply CEP pattern
        PatternStream<EnrichedTradeOrder> patternStream = CEP
                .pattern(
                        enrichedTradeStream.keyBy(EnrichedTradeOrder::getTraderId), rapidTradePattern
                );

        // Generate alerts
        DataStream<String> alerts = patternStream.select((PatternSelectFunction<EnrichedTradeOrder, String>) pattern -> {
            List<EnrichedTradeOrder> trades = pattern.get("rapidTrades");
            return "Alert: " + trades.get(0).getTraderName() + " from " + trades.get(0).getTradingFirm() +
                    " executed " + trades.size() + " rapid trades within 5 seconds. Balance: $" + trades.get(0).getTraderBalance();
        });

        // Print alerts
        alerts.print();

        env.execute("Trader Rapid Trading Detection with Enrichment");
    }
}