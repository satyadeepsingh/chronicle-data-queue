package org.tutorials.chronicle.consumer;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.values.Values;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tutorials.chronicle.queue.consumer.PricingAndRiskEngine;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PricingAndRiskEngineTest {

    // Clean up the persisted map file before each test run
    @BeforeEach
    public void cleanup() {
        File mapFile = new File("ingress/map/fx-risk-map.dat");
        if (mapFile.exists()) {
            mapFile.delete();
        }
    }

    @Test
    public void testPricingAndRiskEngineIntegration() throws Exception {
        // Define test data
        String ccyPair = "EUR/USD "; // Padded with a space to make it exactly 8 bytes
        String price = "1.0550";
        String amount = "1000000";
        String uniqueId = java.util.UUID.randomUUID().toString();
        
        // Construct the full string payload
        String fxTradeMsg = ccyPair + price + "|" + amount + "|" + uniqueId;
        byte[] payload = fxTradeMsg.getBytes(StandardCharsets.UTF_8);

        // 1. WRITE a message to the INGRESS queue, simulating the ingestion service
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder("ingress/queue").build()) {
            ExcerptAppender appender = queue.createAppender();
            appender.writeBytes(b -> {
                b.writeInt(payload.length); // Length prefix
                b.writeSkip(1); 
                b.write(payload);
                b.writeSkip(1);
                b.writeByte((byte) '|');
                b.writeLong(System.nanoTime());
            });
        }

        // 2. RUN the PricingAndRiskEngine in a background thread
        Thread engineThread = new Thread(() -> {
            try {
                PricingAndRiskEngine.main(new String[]{});
            } catch (Exception e) {
                // This will be noisy on shutdown, can be ignored
            }
        });
        engineThread.setDaemon(true);
        engineThread.start();

        // Allow the engine a moment to process the message from the queue
        Thread.sleep(2000); // Increased sleep to be safe

        // 3. VERIFY the output in the fx-risk-map
        long expectedExposure = Long.parseLong(amount);
        
        long keyToLookup = java.nio.ByteBuffer.wrap(ccyPair.getBytes(StandardCharsets.UTF_8)).getLong();

        try (ChronicleMap<LongValue, LongValue> riskMap = ChronicleMap
                .of(LongValue.class, LongValue.class)
                .name("fx-risk-map")
                .entries(10_000).createPersistedTo(new File("ingress/map/fx-risk-map.dat"))) {

            // Use a heap instance for testing so we don't need to manually wire up a BytesStore
            LongValue searchKey = Values.newHeapInstance(LongValue.class);
            searchKey.setValue(keyToLookup);

            LongValue storedExposure = riskMap.get(searchKey);

            int retries = 5;
            while (storedExposure == null && retries-- > 0) {
                Thread.sleep(500);
                storedExposure = riskMap.get(searchKey);
            }

            assertNotNull(storedExposure, "Exposure was not found in the risk map for key: " + keyToLookup);
            assertEquals(expectedExposure, storedExposure.getValue());
        }

        // 4. Clean up
        engineThread.interrupt();
    }
}