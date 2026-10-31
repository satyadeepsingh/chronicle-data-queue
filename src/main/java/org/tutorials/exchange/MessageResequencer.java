package org.tutorials.exchange;

@FunctionalInterface
interface Listener {

    void onDelivery(long payload);
}
public class MessageResequencer {

    private final long[] payloads;
    private final long[] stamp;
    private final int MAX_BUFFER_WORD;
    private final long mask;
    private long expected = 1;
    private final Listener listener;


    public MessageResequencer(int MAX_BUFFER_WORD, Listener listener) {
        int maxBufferWord = getNextPowerOfTwo(MAX_BUFFER_WORD);
        this.MAX_BUFFER_WORD = maxBufferWord;
        this.payloads = new long[maxBufferWord];
        this.stamp = new long[maxBufferWord];
        this.mask = maxBufferWord - 1;
        this.listener = listener;

    }

    private int getNextPowerOfTwo(int maxBufferWord) {
        if(maxBufferWord < 2) return 2;
        return 1 << (32 - Integer.numberOfLeadingZeros(maxBufferWord - 1));
    }

    public boolean onMessage(long seq, long payload) {
        if(seq < expected) return false;
        if(seq >= (expected + MAX_BUFFER_WORD)) return false;
        int writeSeq = (int)(seq & mask);
        if(stamp[writeSeq] == seq) return false;

        payloads[writeSeq] = payload;
        stamp[writeSeq] = seq;

        while(stamp[(int)(expected & mask)] == expected) {
            listener.onDelivery(payloads[(int)(expected & mask)]);
            expected++;
        }
        return true;
    }
}
