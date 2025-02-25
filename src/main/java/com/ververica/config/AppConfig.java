package com.ververica.config;

import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AppConfig {
    public static final String BOOTSTRAP_URL = "";

    public static final String STOCKS_TOPIC = "stocks";
    public static final String TRADERS_TOPIC = "traders";
    public static final String TRADE_ORDERS_TOPIC = "tradeorders";
    public static final String MARKET_EVENTS_TOPIC = "marketsevents";

    public static final String CONSUMER_ID = "fx.consumer";

    public static Properties buildProducerProps() {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_URL);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        return buildSecurityProps(properties);
    }

    public static Properties buildSecurityProps(Properties properties) {
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username= password=;");

        return properties;
    }
}