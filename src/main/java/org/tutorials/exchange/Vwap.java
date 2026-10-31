package org.tutorials.exchange;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class Vwap {

    public static final int MAX_SIZE = 20_000;
    private long totalPrice;
    private long totalQty;

    private long version;
    private long writeSequence;
    private final long[] prices;
    private final long[] quantities;
    private final int mask;

    private static VarHandle STATE_HANDLE;
    private static VarHandle v;
    private static VarHandle p;
    private static VarHandle q;

    static {
        MethodHandles.Lookup l = MethodHandles.lookup();
        try {
            v = l.findVarHandle(Vwap.class, "version", long.class);
            p = l.findVarHandle(Vwap.class, "totalPrice", long.class);
            q = l.findVarHandle(Vwap.class, "totalQty", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public Vwap() {
        prices = new long[MAX_SIZE];
        quantities = new long[MAX_SIZE];
        mask = 19_999;
    }

    public void onTick(long price, long qty) {
        long currentSeq = writeSequence;
        v.setVolatile(this, 1L); // writing
        if(currentSeq >= MAX_SIZE) {

        }
    }
}
