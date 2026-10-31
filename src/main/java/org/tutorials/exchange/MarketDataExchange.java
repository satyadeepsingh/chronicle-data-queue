package org.tutorials.exchange;

import org.agrona.collections.IntArrayQueue;
import org.agrona.collections.Long2LongHashMap;

import java.util.Arrays;

public class MarketDataExchange {

    private final int MAX_ORDERS, MAX_TICKS;
    private final long MIN_PRICE, TICK_SIZE;
    // order SoA
    private final long[] orderIds;
    private final long[] orderSizes;
    private final int[] prevSlots;
    private final int[] nextSlots;

    //Price level SoA
    private final int[] headSlots;
    private final int[] tailSlots;
    private final long[] levelSizes;

    private final Long2LongHashMap orderIdToSlot = new Long2LongHashMap(-1);
    private final IntArrayQueue freeSlots;

    private int bestBidLevel = -1;
    private int bestAskLevel = Integer.MAX_VALUE;

    public MarketDataExchange(int MAX_ORDERS, int MAX_TICKS, long minPrice, long tickSize) {
        this.MAX_ORDERS = MAX_ORDERS;
        this.MAX_TICKS = MAX_TICKS;
        this.MIN_PRICE = minPrice;
        this.TICK_SIZE = tickSize;
        this.orderIds = new long[MAX_ORDERS];
        this.orderSizes = new long[MAX_ORDERS];

        this.prevSlots = new int[MAX_ORDERS];
        this.nextSlots = new int[MAX_ORDERS];
        Arrays.fill(this.prevSlots, -1);
        Arrays.fill(this.nextSlots, -1);

        this.headSlots = new int[MAX_TICKS];
        this.tailSlots = new int[MAX_TICKS];
        Arrays.fill(this.headSlots, -1);
        Arrays.fill(this.tailSlots, -1);

        this.levelSizes = new long[MAX_TICKS];

        freeSlots = new IntArrayQueue(MAX_ORDERS, -1);

        for(int i = 0; i < MAX_ORDERS; i++) {
            freeSlots.addInt(i);
        }

    }

    public void addOrder(long orderId, long size, long price, char side) {

        int slot = this.freeSlots.pollInt();
        if(slot == -1) return;
        // add slot to the order grid
        this.orderIds[slot] = orderId;
        this.orderSizes[slot] = size;
        this.orderIdToSlot.put(orderId, slot);

        // add slot to the price level heads and tails
        // and adjust prev and next slots of the order grid
        int priceIdx = getPriceIndex(price);
        if(this.headSlots[priceIdx] == -1) {
            this.headSlots[priceIdx] = slot;
            this.prevSlots[slot] = -1;
        } else {
            int oldTail = this.tailSlots[priceIdx];
            this.prevSlots[slot] = oldTail;
            this.nextSlots[oldTail] = slot;
        }
        this.tailSlots[priceIdx] = slot;
        this.nextSlots[slot] = -1;

        if(side == 'B') {
            bestBidLevel = Math.max(priceIdx, bestBidLevel);
        } else {
            bestAskLevel = Math.min(priceIdx, bestAskLevel);
        }

        this.levelSizes[priceIdx] += size;

    }

    public void cancelOrder(long orderId, long size, long price, char side) {

        long slotL = this.orderIdToSlot.remove(orderId);
        if(slotL == -1L) return;

        int slot = (int)slotL;
        this.orderIds[slot] = -1;
        this.orderSizes[slot] = 0;

        int prev = this.prevSlots[slot];
        int next = this.nextSlots[slot];
        int priceIdx = getPriceIndex(price);
        if(prev != -1) { // this is not head
            this.nextSlots[prev] = next;
        } else { // this is head
            this.headSlots[priceIdx] = next;
        }

        if(next != -1) { // this is not tail
            this.prevSlots[next] = prev;
        } else {
            this.tailSlots[priceIdx] = prev;
        }

        this.prevSlots[slot] = -1;
        this.nextSlots[slot] = -1;

        this.freeSlots.addInt(slot);
        this.levelSizes[priceIdx] -= size;

        if(side == 'B' && priceIdx == bestBidLevel && this.levelSizes[priceIdx] <= 0) {
            bestBidLevel = scanUpForNextBidIndex(priceIdx);
        } else if(side == 'A' && priceIdx == bestAskLevel && this.levelSizes[priceIdx] <= 0) {
            bestAskLevel = scanDownForNextAskIndex(priceIdx);
        }
    }

    private int scanDownForNextAskIndex(int priceIdx) {
        for(int i = priceIdx + 1; i < MAX_TICKS; i++) {
            if(this.headSlots[i] != -1) return i;
        }
        return Integer.MAX_VALUE;
    }

    private int scanUpForNextBidIndex(int priceIdx) {
       for(int i = priceIdx - 1; i >= 0; i--) {
           if(this.tailSlots[i] != -1) return i;
       }
       return -1;
    }

    private int getPriceIndex(long price) {
        if(price < MIN_PRICE)
            throw new RuntimeException("Price must be greater than the min price being tracked" + this.MIN_PRICE);
        return (int)((price - MIN_PRICE) / TICK_SIZE);

    }

    public void executeAggressiveBuy(long price, long size) {
        int priceIdx = getPriceIndex(price);
        if (priceIdx < bestBidLevel) return;

        while (priceIdx >= bestBidLevel && this.levelSizes[bestBidLevel] > 0 && size > 0) {
            int slot = this.headSlots[bestBidLevel];
            long orderSize = this.orderSizes[slot];
            if (orderSize > size) {
                this.orderSizes[slot] = orderSize - size;
                size = 0;
            } else {
                size -= this.orderSizes[slot];
                this.orderSizes[slot] = 0;
                this.orderIdToSlot.remove(this.orderIds[slot]);
                this.orderIds[slot] = -1;
                int prev = this.prevSlots[slot];
                int next = this.nextSlots[slot];

                if(prev != -1) { // this is not head
                    this.nextSlots[prev] = next;
                } else { // this is head
                    this.headSlots[bestBidLevel] = next;
                }

                if(next != -1) { //this is not tail
                    this.prevSlots[next] = prev;
                } else { // this is tail
                    this.tailSlots[bestBidLevel] = prev;
                }
                bestBidLevel = scanUpForNextBidIndex(priceIdx);
            }
            this.levelSizes[bestBidLevel] -= size;
        }



    }

}