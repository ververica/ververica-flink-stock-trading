package com.ververica.apps.fn;

import com.ververica.models.Stock;
import com.ververica.models.TradeOrder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class TradeExecutionProcessorFn extends KeyedCoProcessFunction<String, Stock, TradeOrder, String> {
    private transient ValueState<TradeOrder> pendingOrderState;
    private transient ValueState<Double> lastPriceState;

    @Override
    public void open(Configuration parameters) {
        pendingOrderState =
                getRuntimeContext().getState(
                        new ValueStateDescriptor<>("pendingOrder", TradeOrder.class));

        lastPriceState =
                getRuntimeContext().getState(
                        new ValueStateDescriptor<>("lastPrice", Double.class));
    }

    @Override
    public void processElement1(Stock stock, Context ctx, Collector<String> out) throws Exception {
        TradeOrder pendingOrder = pendingOrderState.value();
        Double lastPrice = lastPriceState.value();

        if (pendingOrder != null && lastPrice != null) {
            double priceChange = ((stock.getCurrentPrice() - lastPrice) / lastPrice) * 100;
            if (Math.abs(priceChange) > 5.0) {
                out.collect("Price volatility detected! Pausing execution for order " + pendingOrder.getOrderId());
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
            } else {
                out.collect("Executing trade order " + pendingOrder.getOrderId() + " at price $" + stock.getCurrentPrice());
                pendingOrderState.clear();
            }
        }
        lastPriceState.update(stock.getCurrentPrice());
    }

    @Override
    public void processElement2(TradeOrder order, Context ctx, Collector<String> out) throws Exception {
        pendingOrderState.update(order);
        out.collect("Received trade order: " + order.getOrderId() + " for " + order.getSymbol() + " at price: " + order.getPrice());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        TradeOrder pendingOrder = pendingOrderState.value();
        Double lastPrice = lastPriceState.value();

        if (pendingOrder != null && lastPrice != null) {
            double priceChange = ((lastPrice - pendingOrder.getPrice()) / pendingOrder.getPrice()) * 100;
            if (Math.abs(priceChange) > 5.0) {
                out.collect("Price still volatile! Cancelling trade order " + pendingOrder.getOrderId());
            } else {
                out.collect("Price stabilized! Executing trade order " + pendingOrder.getOrderId() + " at price $" + lastPrice);
            }
            pendingOrderState.clear();
        }
    }
}
