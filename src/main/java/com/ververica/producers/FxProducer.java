package com.ververica.producers;

import com.ververica.config.AppConfig;
import com.ververica.datagen.StockMarketDataGenerator;
import com.ververica.models.MarketEvent;
import com.ververica.models.Stock;
import com.ververica.models.TradeOrder;
import com.ververica.models.Trader;
import com.ververica.utils.StreamingUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.ververica.utils.StreamingUtils.closeProducer;

public class FxProducer {
    private static final Logger logger
            = LoggerFactory.getLogger(FxProducer.class);

    public static void main(String[] args) {
        var properties = AppConfig.buildProducerProps();
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "64000");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        logger.info("Starting Kafka Producers  ...");

        var stocksProducer = new KafkaProducer<String, Stock>(properties);
        var tradersProducer = new KafkaProducer<String, Trader>(properties);
        var tradeOrdersProducer = new KafkaProducer<String, TradeOrder>(properties);
        var marketEventsProducer = new KafkaProducer<String, MarketEvent>(properties);

        int totalStocks = 1000000;
        int totalTraders = 10000000;
        int events = 1000000000;

        logger.info("Generating {} stocks ...", totalStocks);
        var count = 0;

        List<Stock> stocks = IntStream
                .range(0, totalStocks)
                .mapToObj(i -> StockMarketDataGenerator.generateStock(i))
                .collect(Collectors.toList());

        for (Stock stock: stocks) {
            StreamingUtils.handleMessage(stocksProducer, AppConfig.STOCKS_TOPIC, stock.getStockId(), stock);
            count++;
            if (count % 1000000 == 0) {
                logger.info("Total so far {}.", count);
            }
        }

        closeProducer(stocksProducer);


        logger.info("Generating {} traders ...", totalTraders);
        count = 0;

        List<Trader> traders = IntStream
                .range(0, totalTraders)
                .parallel()
                .mapToObj(StockMarketDataGenerator::generateTrader)
                .collect(Collectors.toList());
        for (Trader trader: traders) {
            StreamingUtils.handleMessage(tradersProducer, AppConfig.TRADERS_TOPIC, trader.getTraderId(), trader);
            count++;
            if (count % 1000000 == 0) {
                logger.info("Total so far {}.", count);
            }
        }
        closeProducer(tradersProducer);

        logger.info("Generating {} events ...", events);
        Random random = new Random();

        count = 0;
        for (int i = 0; i < events; i++) {
            var tradeOrder = StockMarketDataGenerator.generateTradeOrder(traders, stocks);
            StreamingUtils.handleMessage(tradeOrdersProducer, AppConfig.TRADE_ORDERS_TOPIC, tradeOrder.getOrderId(), tradeOrder);
            count++;
            if (random.nextDouble() < 0.5) {
                MarketEvent marketEvent = StockMarketDataGenerator.generateMarketEvent(stocks);
                StreamingUtils.handleMessage(marketEventsProducer, AppConfig.MARKET_EVENTS_TOPIC, marketEvent.getEventId(), marketEvent);
            }
            if (count % 1000000 == 0) {
                logger.info("Total so far {}.", count);
            }
        }

        logger.info("Closing Producers ...");
        closeProducer(tradeOrdersProducer);
        closeProducer(marketEventsProducer);
    }
}
