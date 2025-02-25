package com.ververica.apps;

import com.ververica.apps.fn.TradeExecutionProcessorFn;
import com.ververica.models.Stock;
import com.ververica.models.TradeOrder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

import static com.ververica.config.AppConfig.buildSecurityProps;
import static com.ververica.utils.StreamingUtils.createStockConsumer;
import static com.ververica.utils.StreamingUtils.createTradeOrderConsumer;

public class SmartTradeExecution {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        var properties = buildSecurityProps(new Properties());

        KafkaSource<Stock> stockSource =  createStockConsumer(properties);
        KafkaSource<TradeOrder> tradeOrderSource =  createTradeOrderConsumer(properties);

        var watermarkStrategy =
                WatermarkStrategy
                        .<TradeOrder>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> event.getOrderTime());

        DataStream<Stock> stockStream = environment
                .fromSource(stockSource, WatermarkStrategy.forMonotonousTimestamps(), "Stocks Source")
                .name("StockSource")
                .uid("StockSource");

        DataStream<TradeOrder> tradeOrderStream = environment
                .fromSource(tradeOrderSource, watermarkStrategy, "Trade Orders Source")
                .name("TradeOrderSource")
                .uid("TradeOrderSource");

        stockStream.keyBy(Stock::getSymbol)
                .connect(tradeOrderStream.keyBy(TradeOrder::getSymbol))
                .process(new TradeExecutionProcessorFn())
                .print();

        environment.execute("Smart Trade Execution");
    }
}
