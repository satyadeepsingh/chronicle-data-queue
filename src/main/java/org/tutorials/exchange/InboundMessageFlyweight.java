package org.tutorials.exchange;

import org.agrona.DirectBuffer;

/**
 * COMPONENT 2: The Inbound Message Flyweight (Agrona)
 * Maps strictly to the 26-byte incoming layout.
 * <h2>Data Model</h2>
 * Assume a continuous byte buffer stream where each inbound message is 26 bytes,
 * structured as follows:
 * <ol>
 *     <li>MessageType (1 byte, char): 'A' (Add), 'M' (Modify), 'C' (Cancel)</li>
 *     <li>Side (1 byte, char): 'B' (Bid), 'A' (Ask)</li>
 *     <li>Price (8 bytes, long): Scaled integer representation (e.g., $5000000$ for $\$50,000.00$)</li>
 *     <li>Size (8 bytes, long): Number of contracts</li>
 *     <li>OrderId (8 bytes, long): Unique identifier for the order</li>
 * </ol>
 */
public class InboundMessageFlyweight {

    public static final int TYPE_OFFSET = 0;       // 1 byte
    public static final int SIDE_OFFSET = 1;       // 1 byte
    public static final int PRICE_OFFSET = 2;      // 8 bytes
    public static final int SIZE_OFFSET = 10;      // 8 bytes
    public static final int ORDER_ID_OFFSET = 18;  // 8 bytes
    public static final int MESSAGE_LENGTH = 26;   // 8 bytes

    private DirectBuffer buffer;
    private int offset;

    public void wrap(DirectBuffer buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
    }

    public byte getMessageType() {
        return buffer.getByte(offset + TYPE_OFFSET);
    }

    public byte getSide() {
        return buffer.getByte(offset + SIDE_OFFSET);
    }

    public long getPrice() {
        return buffer.getLong(offset + PRICE_OFFSET);
    }

    public long getSize() {
        return  buffer.getLong(offset + SIZE_OFFSET);
    }

    public long getOrderId() {
        return buffer.getLong(offset + ORDER_ID_OFFSET);
    }

}
