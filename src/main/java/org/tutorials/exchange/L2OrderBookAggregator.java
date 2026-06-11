package org.tutorials.exchange;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;
import org.agrona.DirectBuffer;

/**
 * COMPONENT 3: The Integrated L2 Aggregator
 */
public class L2OrderBookAggregator {

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
    private final long[] bidLevels;
    private int bestBidIndex = -1; // -1 means book is empty

    //Base price configuration for indexing
    private final long minPriceTarget;
    private final long tickSize;

    private final long[] askLevels;
    private int bestAskIndex = Integer.MAX_VALUE;

    private final int maxPriceLevels;

    public L2OrderBookAggregator(ChronicleMap<Long, OrderState> orderStorage, int maxPriceLevels, long minPriceTarget, long tickSize) {
        this.orderStorage = orderStorage;
        this.bidLevels = new long[maxPriceLevels];
        this.minPriceTarget = minPriceTarget;
        this.tickSize = tickSize;
        this.askLevels = new long[maxPriceLevels];
        this.maxPriceLevels = maxPriceLevels;
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
