package org.tutorials.exchange;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;

class OrderFlyweight {

    private final int SIDE_OFFSET = 0; // 1 byte char
    private final int PRICE_OFFSET = 1; // 8 byte long
    private final int SIZE_OFFSET = 9; // 4 bytes int
    private final int OPERATION_OFFSET = 13;

    private ByteBuffer buffer;
    private int baseOffset;

    public void wrap(ByteBuffer buffer, int baseOffset) {
        this.buffer = buffer;
        this.baseOffset = baseOffset;
    }

    public char getSide() {
        return buffer.getChar(baseOffset + SIDE_OFFSET);
    }

    public long getPrice() {
        return buffer.getLong(baseOffset + PRICE_OFFSET);
    }

    public int getSize() {
        return buffer.getInt(baseOffset + SIZE_OFFSET);
    }

    public char getOperation() {
        return buffer.getChar(baseOffset + OPERATION_OFFSET);
    }
}


public class LimitOrderBook {

    private final OrderFlyweight flyweight = new OrderFlyweight();

    private final long[] bids;
    private final long[] asks;

    private final long[] states;

    private final long MIN_PRICE;
    private final long TICK_SIZE;

    private long bestBid;
    private long bestOffer = Long.MAX_VALUE;

    public LimitOrderBook(int maxTicks, long minPrice, long tickSize) {
        this.bids = new long[maxTicks];
        this.asks = new long[maxTicks];
        this.states = new long[maxTicks * 8];
        this.MIN_PRICE = minPrice;
        this.TICK_SIZE = tickSize;
    }

    private static final VarHandle STATE_HANDLE;

    static {
        STATE_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);
    }

    public void onData(ByteBuffer buffer, int offset) {
        flyweight.wrap(buffer, offset);

        switch (flyweight.getOperation()) {
            case 'A':
                onOrderAdded(flyweight.getPrice(), flyweight.getSize(), flyweight.getSide());
                break;
            case 'C':
                onOrderCanceled(flyweight.getPrice(), flyweight.getSize(), flyweight.getSide());
                break;
            default:
                System.err.printf("Mapping operation doesnt exist: [%s]%n", flyweight.getOperation());
                break;
        }
    }

    private void onOrderCanceled(long price, int size, char side) {
        int index = getArrIndex(price);
        int idx = index * 8;

        long state = (long) STATE_HANDLE.getAcquire(states, idx);
        while (state == 1L) { // this is getting mutated, let it finish first
            Thread.onSpinWait();
            state = (long) STATE_HANDLE.getAcquire(states, idx);
        }

        if(state == 2L) {
            STATE_HANDLE.setVolatile(states, idx, 1L);
            if(side == 'B') {
                bids[index] = bids[index] > 0 ? bids[index] - size : 0;
                if(price == bestBid && bids[index] == 0)
                    bestBid = scanDownForNextBestIndex(index);
            } else {
                asks[index] = asks[index] > 0 ? asks[index] - size : 0;
                if(price == bestOffer && asks[index] == 0)
                    bestOffer = scanUpForNextBestIndex(index);
            }
            STATE_HANDLE.setRelease(states, idx, 2L);
        }

    }

    private long scanUpForNextBestIndex(int index) {
        for(int i = index; i >= 0; i--) {
            if(asks[i] > 0) return getPriceFromIndex(i);
        }
        return 0;
    }

    private long getPriceFromIndex(int index) {
        return (index * TICK_SIZE) + MIN_PRICE;
    }

    private long scanDownForNextBestIndex(int index) {
        for(int i = index; i < bids.length; i++)
            if(bids[i] > 0) return getPriceFromIndex(i);

        return 0;
    }

    private void onOrderAdded(long price, int size, char side) {
        int index = getArrIndex(price);

        int idx = index * 8;
        STATE_HANDLE.setVolatile(states, idx, 1L); // 1 means mutating
        if(side == 'B') {
            bids[index] += size;
            bestBid = Math.max(bestBid, price);
        } else {
            asks[index] += size;
            bestOffer = Math.min(bestOffer, price);
        }
        STATE_HANDLE.setRelease(states, idx, 2L);
    }

    private int getArrIndex(long price) {
        if(price < MIN_PRICE) throw new RuntimeException("Price cannot be less than the tracked min price of " + MIN_PRICE);
        return (int)((price - MIN_PRICE) / TICK_SIZE);
    }
}
