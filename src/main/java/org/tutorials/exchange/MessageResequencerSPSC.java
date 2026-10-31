package org.tutorials.exchange;

public class MessageResequencerSPSC {

    private final int MAX_BUFFER;
    private final long[] payloads;
    private long expected = 1;
    private final long[] stamp;
    SequencerSpSc spscRingBuffer;
    private final int mask;

    public MessageResequencerSPSC(int MAX_BUFFER) {
        int capacity = getNextPowerOfTwo(MAX_BUFFER);
        this.MAX_BUFFER = capacity;
        payloads = new long[capacity];
        stamp = new long[capacity];
        spscRingBuffer = new SequencerSpSc(capacity);
        this.mask = capacity - 1;

    }

    private int getNextPowerOfTwo(int maxBuffer) {
        if(maxBuffer <= 0) throw new IllegalArgumentException("Capacity should be greater than 0");
        if(maxBuffer <= 2) return 2;
        return 1 << (32 - Integer.numberOfLeadingZeros(maxBuffer - 1));
    }

    public void onMessage(long seq, long payload) {
        if(seq < expected) return;
        if(seq >= expected + MAX_BUFFER) return;
        int writeSeq = (int)(seq & mask);
        if(stamp[writeSeq] == seq) return;

        payloads[writeSeq] = payload;
        stamp[writeSeq] = seq;

        while (stamp[(int)(expected & mask)] == expected) {
            if(spscRingBuffer.offer(payloads[(int)(expected & mask)])) expected++;
            else return;
        }
    }


}


