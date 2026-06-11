package org.tutorials.exchange;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

class PaddedSequence {
    //56 bytes of padding before the value
    long p1, p2, p3, p4, p5, p6, p7;

    // the actual sequence value
    volatile long val = 0;

    // 56 bytes of padding after the value
    long p8, p9, p10, p11, p12, p13, p14;

    // Modern java alternative to com.sun.misc.Unsafe for memory barriers
    private static final VarHandle VALUE_HANDLE;

    static {
        try {
            VALUE_HANDLE = MethodHandles.lookup().findVarHandle(PaddedSequence.class, "val", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public long get() {
        return val; // LoadLoadBarrier Volatile Read
    }

    public void lazySet(long newValue) {
        // StoreStore barrier: Cheaper than a volatile write.
        // Guarantees data is written to memory before the sequence is updated,
        // but doesn't force an immediate cache flush.
        VALUE_HANDLE.setRelease(this, newValue);
    }
}

public class SPSCRingBuffer {

    private final int capacity;
    private final int mask;

    // The two padded pointers sitting on isolated cache lines
    private final PaddedSequence head = new PaddedSequence(); // Read by consumer, Written by consumer
    private final PaddedSequence tail = new PaddedSequence(); // Read by producer, written by producer

    // Structure of arrays(SoA) for TradeEvent
    private final long[] executionPrices;
    private final long[] executionQuantities;
    private final long[] sides;

    private long cachedHead;
    private long cachedTail;

    public SPSCRingBuffer(int requestedCapacity) {
        if((requestedCapacity & 1L) != 0) throw new IllegalArgumentException("requestedCapacity must be a power of 2");
        this.capacity = requestedCapacity;
        this.mask = this.capacity - 1;
        this.executionPrices = new long[this.capacity];
        this.executionQuantities = new long[this.capacity];
        this.sides = new long[this.capacity];
    }

    /**
     * Called by producer (THE MATCHING ENGINE THREAD)
     * Returns true if successful
     *
     * @param price
     * @param quantity
     * @param side
     * @return
     */
    public boolean offer(long price, long quantity, byte side) {
        long currentTail = tail.get();
        long wrapPoint = currentTail - capacity;

        if(cachedHead <= wrapPoint){
            cachedHead = head.get();
            if(cachedHead <= wrapPoint) return false; // queue is full
        }


        //1. Calculate array index
        // usually to find index you will use currentTail %
        int index = Math.toIntExact(currentTail & mask);

        executionPrices[index] = price;
        executionQuantities[index] = quantity;
        sides[index] = side;

        // 3. Publish the new tail using a Release barrier (lazySet).
        // This ensures the data written in Step 2 is visible BEFORE the tail increments.
        tail.lazySet(currentTail + 1);
        return  true;
    }

    /**
     * Called by CONSUMER (The settlement/Journaling Thread)
     * To avoid allocating objects, we pass in callback interface to receive the data
     */
    public boolean poll(TradeEventHandler handler) {
        long currentHead = head.get();

        if(currentHead >= cachedTail) {
            cachedTail = tail.get();
            if (currentHead >= cachedTail) return false;
        }

        int index = Math.toIntExact(currentHead & mask);

        long price = executionPrices[index];
        long quantity = executionQuantities[index];
        long side = sides[index];

        handler.onTrade(price, quantity, side);
        return true;
    }

    @FunctionalInterface
    public interface TradeEventHandler {

        void onTrade(long price, long quantity, long side);
    }
}
