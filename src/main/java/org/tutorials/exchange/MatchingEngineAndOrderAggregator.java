package org.tutorials.exchange;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;
import org.agrona.DirectBuffer;

/**
 * COMPONENT 3: The Integrated L2 Aggregator
 */
public class MatchingEngineAndOrderAggregator {

    /**
     * The InboundMessageFlyweight strictly maps to your exact byte offsets.
     * No object instantiation occurs while reading the buffer stream.
     */
    private final InboundMessageFlyweight flyweight = new InboundMessageFlyweight();

    // ChronicleMap for O(1) Zero-GC order lookups
    private final ChronicleMap<Long, OrderState> orderStorage;

    /**
     * Values.newNativeReference(OrderState.class) creates a single proxy object
     * that wraps the memory address managed by Chronicle.
     */
    private final OrderState stateFlyweight = Values.newNativeReference(OrderState.class);

    // BBO Tracking: Price-indexed arrays for a bounded price range.
    // Index = (price - MIN_PRICE) / TICK_SIZE. Value = aggregated volume at that price.
    private final long[] bidLevels; // resting bids
    private int bestBidIndex = -1; // -1 means book is empty

    //Base price configuration for indexing
    private final long minPriceTarget;
    private final long tickSize;

    private final long[] askLevels; // resting asks
    private int bestAskIndex = Integer.MAX_VALUE;

    private final int maxPriceLevels;

    private final TradeListener tradeListener;

    public MatchingEngineAndOrderAggregator(ChronicleMap<Long, OrderState> orderStorage, int maxPriceLevels, long minPriceTarget, long tickSize, TradeListener tradeListener) {
        this.orderStorage = orderStorage;
        this.bidLevels = new long[maxPriceLevels];
        this.minPriceTarget = minPriceTarget;
        this.tickSize = tickSize;
        this.askLevels = new long[maxPriceLevels];
        this.maxPriceLevels = maxPriceLevels;
        this.tradeListener = tradeListener;
    }

    public void onMessage(DirectBuffer buffer, int offset) {
        flyweight.wrap(buffer, offset);

        byte type = flyweight.getMessageType();

        switch (type) {
            case 'A':
                handleAdd();
                break;
            case 'M', 'C':
                handleModifyOrCancel(type);
                break;
            default:
                throw new RuntimeException("Unknown message type: " + type);
        }
    }

    private void handleModifyOrCancel(byte type) {
        long orderId = flyweight.getOrderId();

        if(!orderStorage.containsKey(orderId)) return;

        orderStorage.getUsing(orderId, stateFlyweight);
        long price = stateFlyweight.getPrice();
        long size = stateFlyweight.getSize();
        byte side = stateFlyweight.getSide();

        //2. Remove from chronicle
        if(type == 'C') orderStorage.remove(orderId);

        //3. Update BBO depth array
        int priceIndex = getPriceIndex(price);
        if(side == 'B') {
            bidLevels[priceIndex] -= size;

            //4. THE CRITICAL PATH: Recalculate the BB if top level was depleted
            if(priceIndex == bestBidIndex && bidLevels[bestBidIndex] == 0) {
                bestBidIndex = scanDownForNextBestBid(bestBidIndex);
            }
        }
        if(side == 'A') {
             if(priceIndex < bestBidIndex) {
                bestAskIndex = priceIndex;
            }

            if(priceIndex == bestAskIndex && askLevels[bestAskIndex] == 0) {
                bestAskIndex =  scanUpForNextBestBid(bestAskIndex);
            }
        }
    }

    private void handleAdd() {
        long orderId = flyweight.getOrderId();
        long price = flyweight.getPrice();
        long size = flyweight.getSize();
        byte side = flyweight.getSide();

        //1. Write to chronicleMap
        orderStorage.acquireUsing(orderId, stateFlyweight);
        stateFlyweight.setSide(side);
        stateFlyweight.setPrice(price);
        stateFlyweight.setSize(size);

        //2. Update BB Depth Array
        int priceIndex = getPriceIndex(price);
        if(side == 'B') {
            bidLevels[priceIndex] += size;

            if(priceIndex > bestBidIndex) {
                bestBidIndex = priceIndex;
            }
        }
        if(side == 'A') {
            askLevels[priceIndex] += size;
            if(priceIndex < bestBidIndex) {
                bestAskIndex = priceIndex;
            }
        }
    }

    /**
     * Core Matching engine
     * 3. Visualizing the Crossing Logic
     * Imagine the book is currently configured like this:
     * Array            | PriceLevel    Volume   |      MarketRole
     * -----------------|------------------------|-----------------
     * askLevels[105]   |   $105           10    |   Resting Seller
     * askLevels[104]   |  $104            5     |   Resting Seller (Best Ask)
     * -- SPREAD --     |
     * bidLevels[102]   | $102            8        Resting Buyer (Best Bid)
     * bidLevels[101]   | $101           15        Resting Buyer
     * <h2>Scenario A: Incoming Sell (Ask) at $101 for 12 units</h2>
     * The engine checks side == 'A'. It knows it needs to find buyers,
     * so it looks at bestBidIndex ($102).
     * $102 is higher than the limit price of $101, so a match occurs.
     * It takes all 8 units from bidLevels[102]. bidLevels[102] is now 0.
     * The loop updates bestBidIndex to scan down to the next level, which is $101.
     * The remaining 4 units of the incoming order match against bidLevels[101],
     * drawing its volume down from 15 to 11.The incoming order is now completely filled.
     * <h2>Scenario B: Incoming Buy (Bid) at $105 for 6 units</h2>
     * The engine checks side == 'B'. It knows it needs to find sellers,
     * so it looks at bestAskIndex ($104).
     * $104 is lower than the limit price of $105, so a match occurs.
     * It takes all 5 units from askLevels[104].
     * askLevels[104] is now 0.The loop updates bestAskIndex to scan up to the next level, which is $105.
     * The remaining 1 unit of the incoming order matches against askLevels[105],
     * drawing its volume down from 10 to 9.
     * The incoming order is now completely filled.
     * @param side
     * @param limitPrice
     * @param orderQty
     */
    public void processLimitOrder(byte side, long limitPrice, long orderQty) {
        long remainingQty = orderQty;
        int limitPriceIndex = getPriceIndex(limitPrice);

        if(side == 'A') { // Incoming Aggressive sell matches against resting bids

            // THE MATCHING LOOP: keep crossing the spread until filled or price limit is reached
            while(remainingQty > 0 && bestBidIndex != -1 && bestBidIndex >= limitPriceIndex) {
                long restingBidQty = bidLevels[bestBidIndex];
                long fillQty = Math.min(remainingQty, restingBidQty);

                //1. Emit the trade
                long executionPrice = minPriceTarget + (bestBidIndex * tickSize);
                this.tradeListener.onTrade(executionPrice, fillQty, side);

                remainingQty -= fillQty;
                bidLevels[bestBidIndex] -= fillQty;
                if(bidLevels[bestBidIndex] == 0) {
                    bestBidIndex = scanDownForNextBestBid(bestBidIndex);
                }
            }

            if(remainingQty > 0) {
                addPassiveBid(limitPriceIndex, remainingQty);
            }
        }

        if(side == 'B') { //Incoming Aggressive buy orders matches against resting sells

            //THE MATCHING LOOP
            while(remainingQty > 0 && bestAskIndex != Integer.MAX_VALUE && bestAskIndex <= limitPriceIndex) {
                long restingAskQty = askLevels[bestAskIndex];
                long fillQty = Math.min(remainingQty, restingAskQty);

                //1. Emit the order
                long price = minPriceTarget + (bestAskIndex * tickSize);
                this.tradeListener.onTrade(price, fillQty, side);

                //2. Decrement the engine state
                remainingQty -= fillQty;
                askLevels[bestAskIndex] -= fillQty;

                if(askLevels[bestAskIndex] == 0) {
                    bestAskIndex = scanDownForNextBestBid(bestAskIndex);
                }
            }

            if(remainingQty > 0) {
                addPassiveAsk(limitPriceIndex, remainingQty);
            }
        }
    }

    private void addPassiveAsk(int priceIndex, long remainingQty) {
        askLevels[priceIndex] += remainingQty;
        if(priceIndex < bestAskIndex) {
            bestAskIndex = priceIndex;
        }
    }

    private void addPassiveBid(int priceIndex, long remainingQty) {
        bidLevels[priceIndex] += remainingQty;
        if(priceIndex > bestBidIndex) {
            bestBidIndex = priceIndex;
        }
    }

    private int scanDownForNextBestBid(int currentIndex) {
        // Scans down to find the next active price level. O(1) amortized in dense books
        for(int i = currentIndex - 1; i >= 0; i--) {
            if(bidLevels[i] > 0){
                return i;
            }
        }
        return -1;
    }

    private int scanUpForNextBestBid(int currentIndex) {
        // Scans down to find the next active price level. O(1) amortized in dense books
        for(int i = currentIndex + 1; i < this.maxPriceLevels; i++) {
            if(askLevels[i] > 0){
                return i;
            }
        }
        return Integer.MAX_VALUE;
    }


    private int getPriceIndex(long price) {
        return Math.toIntExact(((price - minPriceTarget) / tickSize));
    }

    private long getBestBidPrice(){
        return bestBidIndex == -1 ? 0L : minPriceTarget + (bestBidIndex * tickSize);
    }

    private long getBestAskPrice() {
        return bestAskIndex == Integer.MAX_VALUE ? 0L : minPriceTarget + (bestAskIndex * tickSize);
    }


}
