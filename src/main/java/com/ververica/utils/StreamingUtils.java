package com.ververica.utils;

import com.ververica.config.AppConfig;
import com.ververica.models.MarketEvent;
import com.ververica.models.Stock;
import com.ververica.models.TradeOrder;
import com.ververica.models.Trader;
import com.ververica.serdes.MarketEventSerdes;
import com.ververica.serdes.StockSerdes;
import com.ververica.serdes.TradeOrderSerdes;
import com.ververica.serdes.TraderSerdes;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamingUtils {
    private static final Logger logger
            = LoggerFactory.getLogger(StreamingUtils.class);

    public static <K,V> void handleMessage(KafkaProducer<K, V> producer, String topic, K key, V value) {
        var record = new ProducerRecord(topic, key, value);
        producer.send(record, (metadata, exception) -> {
            if (exception !=null) {
                logger.error("Error while producing: ", exception);
            } else {
//                logger.info("Successfully stored offset '{}': partition: {} - {}", metadata.offset(), metadata.partition(), metadata.topic());
            }
        });
    }

    public static <K, V> void closeProducer(KafkaProducer<K, V> producer) {
        producer.flush();
        producer.close();
    }

    public static KafkaSource<Stock> createStockConsumer(Properties properties) {
        return KafkaSource.<Stock>builder()
                .setBootstrapServers(AppConfig.BOOTSTRAP_URL)
                .setTopics(AppConfig.STOCKS_TOPIC)
                .setGroupId(AppConfig.CONSUMER_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new StockSerdes())
                .setProperties(properties)
                .build();
    }

    public static KafkaSource<Trader> createTraderConsumer(Properties properties) {
        return KafkaSource.<Trader>builder()
                .setBootstrapServers(AppConfig.BOOTSTRAP_URL)
                .setTopics(AppConfig.TRADERS_TOPIC)
                .setGroupId(AppConfig.CONSUMER_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TraderSerdes())
                .setProperties(properties)
                .build();
    }

    public static KafkaSource<TradeOrder> createTradeOrderConsumer(Properties properties) {
        return KafkaSource.<TradeOrder>builder()
                .setBootstrapServers(AppConfig.BOOTSTRAP_URL)
                .setTopics(AppConfig.TRADE_ORDERS_TOPIC)
                .setGroupId(AppConfig.CONSUMER_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TradeOrderSerdes())
                .setProperties(properties)
                .build();
    }

    public static KafkaSource<MarketEvent> createMarketEventConsumer(Properties properties) {
        return KafkaSource.<MarketEvent>builder()
                .setBootstrapServers(AppConfig.BOOTSTRAP_URL)
                .setTopics(AppConfig.MARKET_EVENTS_TOPIC)
                .setGroupId(AppConfig.CONSUMER_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new MarketEventSerdes())
                .setProperties(properties)
                .build();
    }

//    public static KafkaSink<Alert> createKafkaAlertSink(Properties properties) {
//        return KafkaSink.<Alert>builder()
//                .setBootstrapServers(AppConfig.BOOTSTRAP_URL)
//                .setRecordSerializer(new AlertSerializer(AppConfig.ALERTS_TOPIC))
//                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                .setKafkaProducerConfig(properties)
//                .build();
//    }
}
