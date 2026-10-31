package org.tutorials.exchange;


import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class SequencerSpSc {

    private static class PaddedSeq {
        long p1, p2, p3, p4, p5, p6, p7;
        long value;
        long p8, p9, p10, p11, p12, p13, p14;

        private static VarHandle VALUE_HANDLE;

        static {
            try {
                VALUE_HANDLE = MethodHandles.lookup().findVarHandle(PaddedSeq.class, "value", long.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        public long get() {
            return value;
        }

        public long lazyGet() {
            return (long)VALUE_HANDLE.getAcquire(this);
        }

        public void lazySet(long newValue) {
             VALUE_HANDLE.setRelease(this, newValue);
        }
    }

    PaddedSeq writeSeq = new PaddedSeq();
    PaddedSeq readSeq = new PaddedSeq();

    private final int capacity;
    private final long[] buffer;
    private final int mask;

    public SequencerSpSc(int capacity) {
        if(capacity <= 0 && (capacity & (capacity - 1)) != 0) throw new IllegalArgumentException("Capacity must be multiple of 2");
        this.capacity = capacity;
        buffer = new long[capacity];
        mask = this.capacity - 1;
    }

    public boolean offer(long payload) {
        long currentWrite = writeSeq.get();
        if(currentWrite - readSeq.lazyGet() >= capacity) return false;

        int index = (int)currentWrite & mask;
        this.buffer[index] = payload;
        this.writeSeq.lazySet(currentWrite + 1);
        return true;
    }

    public long poll() {
        long currentRead = readSeq.get();
        if(currentRead == writeSeq.lazyGet()) return -1L;

        int index = (int)currentRead & mask;
        long payload = buffer[index];
        this.readSeq.lazySet(currentRead + 1);
        return payload;
    }
}
