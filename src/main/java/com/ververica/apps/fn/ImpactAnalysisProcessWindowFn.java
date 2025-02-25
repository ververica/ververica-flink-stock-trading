package com.ververica.apps.fn;

import com.ververica.models.MarketEvent;
import com.ververica.models.Stock;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class ImpactAnalysisProcessWindowFn extends ProcessWindowFunction<Tuple2<MarketEvent, Stock>, String, String, TimeWindow> {

    @Override
    public void process(String symbol, Context context, Iterable<Tuple2<MarketEvent, Stock>> elements, Collector<String> out) {
        Set<String> eventTypes = new HashSet<>();
        double cumulativeImpact = 0.0;
        double priceChange;
        double initialPrice = 0.0;
        double finalPrice = 0.0;
        boolean first = true;

        for (Tuple2<MarketEvent, Stock> element : elements) {
            MarketEvent event = element.f0;
            Stock stock = element.f1;

            eventTypes.add(event.getEventType());
            cumulativeImpact += event.getImpactOnPrice();

            if (first) {
                initialPrice = stock.getCurrentPrice();
                first = false;
            }
            finalPrice = stock.getCurrentPrice();
        }

        priceChange = finalPrice - initialPrice;

        String result = String.format("Symbol: %s, Events: %s, Cumulative Impact: %.2f, Actual Price Change: %.2f",
                symbol, eventTypes, cumulativeImpact, priceChange);
        out.collect(result);
    }
}