package org.tutorials.exchange;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TickProcessor {

    private static final long VOD_L = packInstrument("VOL.D");

    private final TickFlyweight flyweight = new TickFlyweight();

    private final SPSCBuffer ringBuffer;

    public TickProcessor(SPSCBuffer ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    static class TickFlyweight {

        private final int TS_OFFSET = 0; // 8 bytes
        private final int INST_ID_OFFSET = 8; // 8 byte ASCII
        private final int PRICE_OFFSET = 16; // 8 bytes long
        private final int SIZE_OFFSET = 24; // 4 bytes

        private ByteBuffer buffer;
        private int baseOffset;

        public void wrap(ByteBuffer bytes, int offset) {
           this.buffer = bytes;
           this.baseOffset = offset;

           this.buffer.order(ByteOrder.BIG_ENDIAN);
        }

        long getTimeStamp() {
            return buffer.getLong(baseOffset + TS_OFFSET);
        }

        long getInstrumentId() {
            return this.buffer.getLong(baseOffset + INST_ID_OFFSET);
        }

        long getPrice() {
            return this.buffer.getLong(baseOffset + PRICE_OFFSET);
        }

        int getSize() {
            return this.buffer.getInt(baseOffset + SIZE_OFFSET);
        }
    }

    public void onData(ByteBuffer buffer, int offset) {
        flyweight.wrap(buffer, offset);
        if(flyweight.getInstrumentId() == VOD_L)
            while(!this.ringBuffer.offer(offset)) Thread.onSpinWait();
    }

    public static long packInstrument(String inst) {
        int len = Math.min(8, inst.length());
        long instrument = 0;
        for(int i = 0; i < 8; i++) {
            long ascii = i < len ? (long)inst.charAt(i) : (long)' ';
            instrument |= (ascii & 0xFF) << (56 - (i * 8));
        }

        return instrument;
    }
}
