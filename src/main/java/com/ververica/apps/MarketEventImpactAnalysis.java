package com.ververica.apps;

import com.ververica.apps.fn.ImpactAnalysisProcessWindowFn;
import com.ververica.models.MarketEvent;
import com.ververica.models.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class MarketEventImpactAnalysis {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment
                = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);

        // Simulated market event stream
        DataStream<MarketEvent> marketEventStream = environment.fromElements(
                new MarketEvent("E1", "AAPL", "Earnings Report", 5.0, System.currentTimeMillis()),
                new MarketEvent("E2", "GOOGL", "Product Launch", 3.0, System.currentTimeMillis() + 5000)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<MarketEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.getEventTime())
        );

        // Simulated stock price stream
        DataStream<Stock> stockPriceStream = environment.fromElements(
                new Stock("S1", "AAPL", "Apple Inc.", 150.0, System.currentTimeMillis() - 600000), // 10 minutes before event
                new Stock("S1", "AAPL", "Apple Inc.", 152.0, System.currentTimeMillis() - 300000), // 5 minutes before event
                new Stock("S1", "AAPL", "Apple Inc.", 155.0, System.currentTimeMillis()), // At event time
                new Stock("S1", "AAPL", "Apple Inc.", 158.0, System.currentTimeMillis() + 300000), // 5 minutes after event
                new Stock("S1", "AAPL", "Apple Inc.", 160.0, System.currentTimeMillis() + 600000), // 10 minutes after event
                new Stock("S2", "GOOGL", "Alphabet Inc.", 2800.0, System.currentTimeMillis() - 600000),
                new Stock("S2", "GOOGL", "Alphabet Inc.", 2825.0, System.currentTimeMillis() - 300000),
                new Stock("S2", "GOOGL", "Alphabet Inc.", 2850.0, System.currentTimeMillis()),
                new Stock("S2", "GOOGL", "Alphabet Inc.", 2875.0, System.currentTimeMillis() + 300000),
                new Stock("S2", "GOOGL", "Alphabet Inc.", 2900.0, System.currentTimeMillis() + 600000)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((stock, timestamp) -> stock.getLastUpdated())
        );

        // Keyed streams
        var keyedMarketEventStream = marketEventStream.keyBy(MarketEvent::getStockSymbol);
        var keyedStockPriceStream = stockPriceStream.keyBy(Stock::getSymbol);

        // Interval join between market events and stock prices
        DataStream<Tuple2<MarketEvent, Stock>> joinedStream = keyedMarketEventStream
                .intervalJoin(keyedStockPriceStream)
                .between(Time.minutes(-10), Time.minutes(10))
                .process(new ProcessJoinFunction<MarketEvent, Stock, Tuple2<MarketEvent, Stock>>() {
                    @Override
                    public void processElement(MarketEvent event, Stock stock, Context ctx, Collector<Tuple2<MarketEvent, Stock>> out) {
                        out.collect(new Tuple2<>(event, stock));
                    }
                });

        // Apply a tumbling window to the joined stream
        DataStream<String> impactAnalysisStream = joinedStream
                .keyBy(tuple -> tuple.f0.getStockSymbol())
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .process(new ImpactAnalysisProcessWindowFn());

        impactAnalysisStream.print();

        environment.execute("Market Event Impact Analysis");
    }
}
