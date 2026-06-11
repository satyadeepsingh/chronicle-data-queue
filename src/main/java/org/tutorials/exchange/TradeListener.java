package org.tutorials.exchange;

@FunctionalInterface
public interface TradeListener {

    void onTrade(long executionPrice, long fillQuantity, byte side);
}
