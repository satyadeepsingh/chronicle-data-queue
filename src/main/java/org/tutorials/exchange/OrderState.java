package org.tutorials.exchange;


/**
 * COMPONENT 1: The Off-Heap Resting State (Chronicle Value Interface)
 * Chronicle generates the proxy class at runtime to map these to off-heap memory.
 */
public interface OrderState {

    byte getSide();
    void setSide(byte side);

    long getPrice();
    void setPrice(long price);

    long getSize();
    void setSize(long size);
}