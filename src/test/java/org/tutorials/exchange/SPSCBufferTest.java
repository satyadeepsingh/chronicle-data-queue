package org.tutorials.exchange;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SPSCBufferTest {

    @Test
    void testSPSCBufferProducerConsumer() throws InterruptedException {

        long[] prices = new long[100_000];
        AtomicInteger ai = new AtomicInteger(0);
        SPSCBuffer spscBuffer = new SPSCBuffer(8);

        ExecutorService producer = Executors.newFixedThreadPool(1);

        ExecutorService consumer = Executors.newFixedThreadPool(1);

        CountDownLatch produced = new CountDownLatch(1);

        CountDownLatch consumed = new CountDownLatch(1);

        consumer.submit(() -> {
            int index = 0;
            while (index < 100_000) {
                long val = spscBuffer.poll();
                if (val == -1) Thread.onSpinWait();
                else {
                    prices[index++] = val;
                }
            }
            consumed.countDown();
        });

        producer.submit(() -> {
            for (int i = 0; i < 100_000; i++) {
                while(!spscBuffer.offer(i)) {
                    Thread.onSpinWait();
                }
            }
            produced.countDown();
        });



        boolean p = produced.await(2, TimeUnit.SECONDS);
        boolean c = consumed.await(2, TimeUnit.SECONDS);

        producer.shutdown();
        consumer.shutdown();

        assertTrue(p & c);
        assertEquals(100_000, prices.length);

        for (int i = 0; i < 100_000; i++) {
            assertEquals(i, prices[i]);
        }



    }
}
