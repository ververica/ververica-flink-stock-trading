package com.ververica.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedTradeOrder extends TradeOrder  {
    private String traderName;
    private String tradingFirm;
    private double traderBalance;

    public EnrichedTradeOrder(TradeOrder tradeOrder, Trader trader) {
        super(tradeOrder.getOrderId(), tradeOrder.getTraderId(), tradeOrder.getSymbol(), tradeOrder.getQuantity(), tradeOrder.getPrice(), tradeOrder.getOrderType(), tradeOrder.getOrderTime());
        this.traderName = trader.getName();
        this.tradingFirm = trader.getTradingFirm();
        this.traderBalance = trader.getBalance();
    }

    @Override
    public String toString() {
        return super.toString() + " | Trader Info: { Name: " + traderName + ", Firm: " + tradingFirm + ", Balance: " + traderBalance + " }";
    }
}
