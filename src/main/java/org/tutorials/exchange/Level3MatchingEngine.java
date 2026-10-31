package org.tutorials.exchange;

import java.util.HashMap;
import java.util.Map;

/**
 * L3 Order Book: FIFO Matching Engine.
 * Arrays represent price levels. Nodes represent individual orders.
 */
public class Level3MatchingEngine {

    static class Order {
        long orderId;
        int price; // Integer mapping to array index Index = (OrderPrice - MinimumPrice) / TickSize
        long quantity;
        boolean isBuy;

        Order prev; // Doubly-Linked List pointers for O(1) removal
        Order next;

        public Order(long orderId, int price, long quantity, boolean isBuy) {
            this.orderId = orderId;
            this.price = price;
            this.quantity = quantity;
            this.isBuy = isBuy;
        }
    }

    private final int MAX_TICKS; // Total allowable price range

    // Arrays acting as the Head and Tail pointers for the Linked List at each price tick
    private final Order[] levelHeads;
    private final Order[] levelTails;

    // O(1) lookup to cancel an order instantly. (Must be Agrona Int2ObjectHashMap in prod)
    private final Map<Long, Order> orderStorage = new HashMap<>();

    public Level3MatchingEngine(int maxTicks) {
        this.MAX_TICKS = maxTicks;
        this.levelHeads = new Order[maxTicks];
        this.levelTails = new Order[maxTicks];
    }

    public void placeOrder(Order order) {
        orderStorage.put(order.orderId, order);
        int priceIndex = order.price;

        // 1. Insert at the TAIL of the line (Strict FIFO priority)
        if (levelHeads[priceIndex] == null) {
            levelHeads[priceIndex] = order;
            levelTails[priceIndex] = order;
        } else {
            Order oldTail = levelTails[priceIndex];
            oldTail.next = order;
            order.prev = oldTail;
            levelTails[priceIndex] = order;
        }

        // Matcher logic (crossing the spread) would execute here before resting the order.
    }

    /**
     * $O(1)$ Cancellation.
     * This is the exact reason an L3 book uses a Doubly-Linked List.
     */
    public void cancelOrder(long orderId) {
        Order order = orderStorage.remove(orderId);
        if (order == null) return; // Already filled or doesn't exist

        int priceIndex = order.price;

        // Pointer manipulation to rip the node out of the queue in O(1) time
        if (order.prev != null) {
            order.prev.next = order.next;
        } else {
            // It was the head of the line
            levelHeads[priceIndex] = order.next;
        }

        if (order.next != null) {
            order.next.prev = order.prev;
        } else {
            // It was the tail of the line
            levelTails[priceIndex] = order.prev;
        }

        // Null out pointers to assist GC (or return to Object Pool)
        order.prev = null;
        order.next = null;
    }
}
