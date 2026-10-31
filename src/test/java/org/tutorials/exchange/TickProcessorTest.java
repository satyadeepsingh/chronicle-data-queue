package org.tutorials.exchange;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TickProcessorTest {

    @Test
    void tickProcessorNominal() throws InterruptedException {
        SPSCBuffer ringBuffer = new SPSCBuffer(1025);
        TickProcessor processor = new TickProcessor(ringBuffer);
        long[] latencies = new long[(1024 / 28) + (1024 % 28)];
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

        ExecutorService producer = Executors.newSingleThreadExecutor();
        CountDownLatch produced = new CountDownLatch(1);

        CountDownLatch startGun = new CountDownLatch(1);

        producer.submit(() -> {
            try {
                startGun.await();

                for(int offset = 0; offset < (1024 - 28); offset+=28) {
                    buffer.putLong(offset, System.nanoTime());
                    buffer.putLong(offset + 8, TickProcessor.packInstrument("VOL.D"));
                    buffer.putLong(offset + 16, offset * 10L);
                    buffer.putInt(offset + 24, offset * 40);
                    processor.onData(buffer, offset);
                }
                produced.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        ExecutorService consumer = Executors.newSingleThreadExecutor();
        CountDownLatch consumed = new CountDownLatch(1);

        consumer.submit(() -> {
            try {
                startGun.await();
                int index = 0;
                for (int i = 0; i < (1024 - 28); i+=28) {
                    int offset = (int)ringBuffer.poll();
                    while (offset == -1) {
                        Thread.onSpinWait();
                        offset = (int) ringBuffer.poll();
                    }
                    latencies[index++] = System.nanoTime() - buffer.getLong(0);
                    assertEquals(TickProcessor.packInstrument("VOL.D"), buffer.getLong(offset + 8));
                    assertEquals(offset * 10L, buffer.getLong(offset + 16));
                    assertEquals(offset * 40, buffer.getInt(offset + 24));
                }

                consumed.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        });

        startGun.countDown();
        assertTrue(produced.await(20, TimeUnit.SECONDS));
        assertTrue(consumed.await(20, TimeUnit.SECONDS));
        producer.shutdown();
        consumer.shutdown();

        for (int i = 0; i < latencies.length; i++) {
            System.out.printf("times: [%d]ns.%n", latencies[i]);
        }
    }
}
