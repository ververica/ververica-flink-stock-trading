package com.ververica.apps;

import com.ververica.models.TradeOrder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.*;

import static com.ververica.config.AppConfig.buildSecurityProps;
import static com.ververica.utils.StreamingUtils.createTradeOrderConsumer;

public class PriceSpikeDetection {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment environment
                = StreamExecutionEnvironment.getExecutionEnvironment();

        var properties = buildSecurityProps(new Properties());

        KafkaSource<TradeOrder> tradeOrderSource =  createTradeOrderConsumer(properties);

        var watermarkStrategy =
                WatermarkStrategy
                        .<TradeOrder>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> event.getOrderTime());


        DataStream<TradeOrder> tradeOrderStream = environment
                .fromSource(tradeOrderSource, watermarkStrategy, "Trade Orders Source")
                .name("TradeOrderSource")
                .uid("TradeOrderSource");

        // Key the stream by stock symbol
        KeyedStream<TradeOrder, String> keyedStream = tradeOrderStream.keyBy(TradeOrder::getSymbol);

        // Define the pattern: an initial trade followed by a subsequent trade with a significant price increase within 5 minutes
        Pattern<TradeOrder, ?> priceSpikePattern = Pattern.<TradeOrder>begin("initialOrder")
                .next("subsequentOrder")
                .where(new IterativeCondition<TradeOrder>() {
                    @Override
                    public boolean filter(TradeOrder currentOrder, Context<TradeOrder> ctx) throws Exception {
                        for (TradeOrder initialOrder : ctx.getEventsForPattern("initialOrder")) {
                            double priceIncrease = currentOrder.getPrice() - initialOrder.getPrice();
                            if (priceIncrease / initialOrder.getPrice() > 0.05) { // 5% price increase threshold
                                return true;
                            }
                        }
                        return false;
                    }
                }).within(Time.minutes(5));

        PatternStream<TradeOrder> patternStream =
                CEP.pattern(keyedStream, priceSpikePattern);

        // Select and process matching event sequences
        DataStream<String> alerts = patternStream.select(
                new PatternSelectFunction<TradeOrder, String>() {
                    @Override
                    public String select(Map<String, List<TradeOrder>> pattern) {
                        TradeOrder initialOrder = pattern.get("initialOrder").get(0);
                        TradeOrder subsequentOrder = pattern.get("subsequentOrder").get(0);
                        return String.format(
                                "Price spike detected for symbol %s: Order %s at $%.2f followed by Order %s at $%.2f within 5 minutes.",
                                initialOrder.getSymbol(),
                                initialOrder.getOrderId(),
                                initialOrder.getPrice(),
                                subsequentOrder.getOrderId(),
                                subsequentOrder.getPrice()
                        );
                    }
                }
        );

        // Output alerts to the console
        alerts.print().name("Price Spike Alerts");

        // Execute the Flink job
        environment.execute("Price Spike Detection Job");
    }
}