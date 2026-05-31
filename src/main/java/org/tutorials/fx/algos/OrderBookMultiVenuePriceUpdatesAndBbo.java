package org.tutorials.fx.algos;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

public class OrderBookMultiVenuePriceUpdatesAndBbo {

    // --- 1. System Constants & Fixed-Point Math ---
    public static final long PRICE_MULTIPLIER = 100_000L;
    private static final int MAX_VENUES = 3;
    private static final int MAX_INST = 10;


    // CACHE LINE PADDINGS to avoid false sharing
    // A standard x86 L1 cache line is 64 bytes (8 longs).
    // By multiplying the index by 8, we ensure each instrument's sequence
    // lives on its own dedicated cache line, completely eliminating False Sharing.
    public static final int CACHE_LINE_PADDINGS = 8;

    // --- 2. Ingress-Only State (No concurrency protection needed) ---
    // Flat 1D arrays simulating a 2D matrix: index = (instrumentId * MAX_VENUES) + venueId
    private final long[] venueBidPrices = new long[MAX_INST * MAX_VENUES];
    private final long[] venueBidSizes  = new long[MAX_INST * MAX_VENUES];
    private final long[] venueOfferPrices = new long[MAX_INST * MAX_VENUES];
    private final long[] venueOfferSizes  = new long[MAX_INST * MAX_VENUES];

    //Sequence Array manipulated by VarHandle
    private final long[] sequences = new long[MAX_INST * CACHE_LINE_PADDINGS];
    private static final VarHandle SEQ_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);

    private final long[] bboBidPrice = new long[MAX_INST];
    private final long[] bboBidSize = new long[MAX_INST];
    private final long[] bboAskPrice = new long[MAX_INST];
    private final long[] bboAskSize = new long[MAX_INST];

    public OrderBookMultiVenuePriceUpdatesAndBbo() {
        Arrays.fill(bboAskPrice, Long.MAX_VALUE);
    }

    /**
     * Packs an 8-character string (e.g., "EUR/USD ") into a single 64-bit primitive long.
     * Use this at the network edge to bypass String allocation.
     */
    public static long packInstrument(CharSequence c) {
        // example 'EUR/GBP '
        long result = 0;
        int len = Math.min(c.length(), 8);
        for(int i = 0; i < 8; i++) {
            long asciiChar = i < len ? ((byte)c.charAt(i)) : (byte)' ';
            result |= (asciiChar & 0xFF) << (56 - (i * 8));
        }
        return result;
    }
    /**
     * INGRESS THREAD (WRITER)
     * 100% Lock-Free. Never Spins, never blocks, never allocates
     */
    public void onTick(long bidPrice, long bidSize, long askPrice, long askSize, int venueId, int instrumentId) {
        int matrixId = instrumentId * MAX_INST;
        int level = matrixId + venueId;

        venueBidPrices[level] = bidPrice;
        venueBidSizes[level] = bidSize;
        venueOfferPrices[level] = askPrice;
        venueOfferSizes[level] = askSize;

        recalculateBbo(instrumentId);
    }


    public void recalculateBbo(int instrumentId) {

        long bestBid = 0, bestBidSize = 0, bestAsk = Long.MAX_VALUE, bestAskSize = 0;

        int offset = instrumentId * MAX_VENUES;
        int limit  = offset + MAX_VENUES;

        for(int i = offset; i < limit; i++ ) {

            if(bestBid < venueBidPrices[i]) {
                bestBid = venueBidPrices[i];
                bestBidSize = venueBidSizes[i];
            } else if(bestBid == venueBidPrices[i] && bestBid != 0) {
                bestBidSize += venueBidSizes[i];
            }

            if(bestAsk > venueOfferPrices[i]) {
                bestAsk = venueOfferPrices[i];
                bestAskSize = venueOfferSizes[i];
            } else if(bestAsk == venueOfferPrices[i] && venueOfferPrices[i] != Long.MAX_VALUE) {
                bestAskSize += venueOfferSizes[i];
            }
        }

        int seqIdx = instrumentId * CACHE_LINE_PADDINGS;

        //Get current sequence (Plain read is fine here because this thread is the sole writer)
        long currentSequence = (long) SEQ_HANDLE.getOpaque(sequences, seqIdx);

        // 1. Mark as updating(Mark sequence ODD)
        // setVolatile guarantees that the plain array writes below
        // cannot be reordered by the compiler or the CPU to happen before this flag is raised
        SEQ_HANDLE.setVolatile(sequences, seqIdx, currentSequence + 1);

        // 2. Write Data (Plain writes - extremely fast)
        bboBidPrice[instrumentId] = bestBid;
        bboBidSize[instrumentId] = bestBidSize;
        bboAskPrice[instrumentId] = bestAsk;
        bboAskSize[instrumentId] = bestAskSize;

        // 2. Mark s Complete (EVEN)
        // setRelease acts as a StoreStore memory barrier. It guarantees all plain writes
        // above are flushed to main memory BEFORE this even sequence number becomes visible.
        SEQ_HANDLE.setRelease(sequences, seqIdx, currentSequence + 2);
    }

    /**
     * STRATEGY THREAD (Reader)
     * Optimistic Concurrency. Spins only if it collides with a write in progress.
     * Uses a mutable flyweight object to ensure zero allocation on the read path.
     */
    public void getConsistentBbo(int instrumentId, BboSnapshot targetSnapshot) {

        int seqIdx = instrumentId * CACHE_LINE_PADDINGS;
        long seq1, seq2=0;
        long bp=0, bs=0, ap=0, as=0;

        do {
            // 1. Read sequence(Acquire memory barrier ensures we see latest memory)
            seq1 = (long) SEQ_HANDLE.getAcquire(sequences, seqIdx);

            // If sequence is ODD, the ingress thread is actively mutating the data.
            // Spin and yield pipeline resources using x86 PAUSE
            if((seq1 & 1L) != 0L) {
                Thread.onSpinWait();
                continue;
            }
            // 2. Read Data (Plain reads)
            bp = bboBidPrice[instrumentId];
            bs = bboBidSize[instrumentId];
            ap = bboAskPrice[instrumentId];
            as = bboAskSize[instrumentId];

            // 3. Read sequence again to verify atomicity
            // A secondary acquire fence prevents the data reads from floating below the sequence check
            seq2 = (long) SEQ_HANDLE.getAcquire(sequences, seqIdx);

            // If the sequences don't match, the ingress thread updated the data
            // while we were reading it. Loop back and try again.
        } while (seq1 != seq2);

        targetSnapshot.update(bp, bs, ap, as);
    }

    public static final class BboSnapshot {
        public long bid;
        public long bidSize;
        public long ask;
        public long askSize;

        public void update(long bid, long bidSize, long ask, long askSize) {
            this.bid = bid;
            this.bidSize = bidSize;
            this.ask = ask;
            this.askSize = askSize;
        }
    }


}
