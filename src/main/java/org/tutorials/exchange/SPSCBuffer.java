package org.tutorials.exchange;

import org.agrona.DirectBuffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

class PaddedSeq {
    private long p1, p2, p3, p4, p5, p6, p7;

    long value;

    private long p8, p9, p10, p11, p12, p13, p14;

    private static VarHandle SEQ_HANDLE;

    static {
        try {
            SEQ_HANDLE = MethodHandles.lookup().findVarHandle(PaddedSeq.class, "value", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    long get() { // plain read
        return (long) SEQ_HANDLE.getOpaque(this);
    }

    long lazyGet() {
        return (long) SEQ_HANDLE.getAcquire(this);
    }

    void lazySet(long newVal) {
        SEQ_HANDLE.setRelease(this, newVal);
    }

}

public class SPSCBuffer {

    private PaddedSeq readSeq = new PaddedSeq();
    private PaddedSeq writeSeq = new PaddedSeq();

    private final long[] buffer;

    private final int mask;

    public SPSCBuffer(int capacity) {
        int len = getNextPowerOfTwo(capacity);
        this.buffer = new long[len];
        this.mask = len - 1;
    }

    public boolean offer(long price) {
        long currentWriteSeq = writeSeq.get();
        if(currentWriteSeq - readSeq.lazyGet() >= buffer.length) return false;

        this.buffer[(int)(currentWriteSeq & mask)] = price;

        this.writeSeq.lazySet(currentWriteSeq + 1);
        return true;
    }

    public long poll() {
        long currentReadSeq = readSeq.get();
        if(currentReadSeq == writeSeq.lazyGet()) return -1L;

        long curr = this.buffer[(int)(currentReadSeq & mask)];
        this.readSeq.lazySet(currentReadSeq + 1);
        return curr;
    }

    private int getNextPowerOfTwo(int capacity) {
        if(capacity <= 1) return 2;
        return 1 << (32 - Integer.numberOfLeadingZeros(capacity - 1));
    }
}
