package org.tutorials.exchange;

/**
 * L2 Order Book: Aggregated depth.
 * Index 0 is ALWAYS the Best Bid / Best Ask (Inside Market).
 */
public class Level2DepthBook {
    private final int MAX_DEPTH;

    // Parallel arrays. bidPrices[0] corresponds to bidQuantities[0]
    private final long[] bidPrices;
    private final long[] bidQuantities;

    private final long[] askPrices;
    private final long[] askQuantities;

    public Level2DepthBook(int maxDepth) {
        this.MAX_DEPTH = maxDepth;
        this.bidPrices = new long[maxDepth];
        this.bidQuantities = new long[maxDepth];
        this.askPrices = new long[maxDepth];
        this.askQuantities = new long[maxDepth];
    }

    /**
     * Process an incoming market data tick.
     */
    public void updateLevel(boolean isBid, long price, long quantity) {
        long[] prices = isBid ? bidPrices : askPrices;
        long[] quantities = isBid ? bidQuantities : askQuantities;

        // 1. Level depletion (Quantity = 0)
        if (quantity == 0) {
            removeLevel(prices, quantities, price);
            return;
        }

        // 2. Find existing level or insertion point
        for (int i = 0; i < MAX_DEPTH; i++) {
            if (prices[i] == price) {
                // In-place memory overwrite. O(1)
                quantities[i] = quantity;
                return;
            }

            // Bids sort descending (highest first). Asks sort ascending (lowest first).
            boolean isBetterPrice = isBid ? (price > prices[i]) : (price < prices[i] || prices[i] == 0);

            if (isBetterPrice) {
                insertLevel(prices, quantities, i, price, quantity);
                return;
            }
        }
    }

    // Hardware Sympathy: System.arraycopy compiles to SIMD native instructions.
    private void insertLevel(long[] prices, long[] quantities, int index, long price, long quantity) {
        if (index < MAX_DEPTH - 1) {
            // Shift everything DOWN to make room for the new better price
            System.arraycopy(prices, index, prices, index + 1, MAX_DEPTH - index - 1);
            System.arraycopy(quantities, index, quantities, index + 1, MAX_DEPTH - index - 1);
        }
        prices[index] = price;
        quantities[index] = quantity;
    }

    private void removeLevel(long[] prices, long[] quantities, long price) {
        for (int i = 0; i < MAX_DEPTH; i++) {
            if (prices[i] == price) {
                if (i < MAX_DEPTH - 1) {
                    // Shift everything UP to fill the gap of the deleted price
                    System.arraycopy(prices, i + 1, prices, i, MAX_DEPTH - i - 1);
                    System.arraycopy(quantities, i + 1, quantities, i, MAX_DEPTH - i - 1);
                }
                prices[MAX_DEPTH - 1] = 0;
                quantities[MAX_DEPTH - 1] = 0;
                return;
            }
        }
    }
}