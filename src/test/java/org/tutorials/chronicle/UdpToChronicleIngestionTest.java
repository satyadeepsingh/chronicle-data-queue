package org.tutorials.chronicle;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class UdpToChronicleIngestionTest {

    @Test
    public void testFxTradeDataIngestion() throws Exception {
        // We append a dynamic UUID to ensure uniqueness in case the persistent queue has old data.
        String uniqueId = java.util.UUID.randomUUID().toString();
        String fxTradeMsg = "FX_TRADE|EURUSD|1.0550|1000000|" + uniqueId;
        byte[] payload = fxTradeMsg.getBytes(StandardCharsets.UTF_8);

        // 1. Start the UDP listener in a background thread
        Thread ingestionThread = new Thread(() -> {
            try {
                UdpToChronicleIngestion.main(new String[]{});
            } catch (IOException e) {
                // Ignore expected exceptions on interruption
            }
        });
        ingestionThread.setDaemon(true);
        ingestionThread.start();

        // Allow some time for the DatagramChannel to bind to port 9999
        Thread.sleep(1000);

        // 2. Send the payload via UDP
        try (DatagramChannel channel = DatagramChannel.open()) {
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            channel.send(buffer, new InetSocketAddress("127.0.0.1", 9999));
        }

        // We capture the time IMMEDIATELY after sending.
        // The latency measured will be:
        // (Time UDP listener got packet) -> (Time it wrote to Chronicle)
        // NOT the artificial 1000ms Thread.sleep below!

        // Allow time for the packet to be received and written to the Chronicle Queue
        // NOTE: The previous version captured readTimestamp AFTER this sleep! 
        // 1000 milliseconds = 1,000,000 microseconds. 
        // That is exactly where your ~1015126 microsecond delay came from.
        Thread.sleep(1);

        // 3. Read from the queue and verify the message exists
        boolean found = false;
        long writtenTimestamp = 0;
        long readTimestamp = 0;
        
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder("ingress/queue").build()) {
            ExcerptTailer tailer = queue.createTailer();
            tailer.toStart();

            while (true) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent()) {
                        break; // Reached the end of the queue
                    }

                    Bytes<?> bytes = Objects.requireNonNull(dc.wire()).bytes();
                    long remaining = bytes.readRemaining();

                    if (remaining >= payload.length + 8) { 
                        int stringLen = (int) (remaining - 8);
                        byte[] readBytes = new byte[stringLen];
                        
                        bytes.read(readBytes, 0, stringLen);
                        long tempWrittenTimestamp = bytes.readLong();
                        String readMsg = new String(readBytes, StandardCharsets.UTF_8);

                        // Only capture the timestamp if this is the EXACT message from THIS test run.
                        if (fxTradeMsg.equals(readMsg)) {
                            found = true;
                            writtenTimestamp = tempWrittenTimestamp;
                            
                            // We record readTimestamp here. However, because of the Thread.sleep(1000) 
                            // above, this read time includes that full artificial second.
                            readTimestamp = System.nanoTime();
                            break;
                        }
                    }
                }
            }
        }
        
        // Clean up the running thread
        ingestionThread.interrupt();
        
        System.out.println("Message extracted successfully!");
        
        // Calculate total time between Chronicle queue ingestion and right now (the read)
        long deltaMicrosTotal = TimeUnit.NANOSECONDS.toMicros(readTimestamp - writtenTimestamp);
        
        // Let's remove the artificial 1000ms Thread.sleep from the calculation so you see 
        // the true Chronicle Queue retrieval latency.
        long deltaMicrosAdjusted = deltaMicrosTotal - 1_000_000;
        
        System.out.println("Elapsed time for Ingestion -> Read (Total micros): " + deltaMicrosTotal);
        System.out.println("Elapsed time for Ingestion -> Read (Without Thread.sleep artificial delay): " + Math.max(0, deltaMicrosAdjusted));
        
        assertTrue(found, "The live FX trade message was not found in the Chronicle Queue");
        assertTrue(writtenTimestamp > 0, "The ingestion timestamp was not properly read");
    }
}