package org.tutorials.chronicle.queue.consumer;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.DocumentContext;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class PricingAndRiskEngine {

    public static ChronicleMap<LongValue, LongValue> riskMap;
    private static final LongValue searchKeyFlyweight = Values.newNativeReference(LongValue.class);
    private static final LongValue exposureFlyweight = Values.newNativeReference(LongValue.class);

    public static void main(String[] args) throws IOException {

        riskMap = ChronicleMap.of(LongValue.class, LongValue.class)
                .name("fx-risk-map")
                .entries(10_000)
                .createPersistedTo(new File("ingress/map/fx-risk-map.dat"));

        try (ChronicleQueue ingressQueue = ChronicleQueue
                .singleBuilder("ingress/queue")
                .rollCycle(RollCycles.FAST_DAILY)
                .build()) {
            ExcerptTailer tailer = ingressQueue.createTailer();

            System.out.println("Starting pricing engine hot loop..");

            while (true) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent()) {
                        continue;
                    }

                    Bytes<?> bytes = Objects.requireNonNull(dc.wire()).bytes();

                    // The test writes a manual length and skip, let's consume them.
                    bytes.readInt(); 
                    bytes.readSkip(1);

                    // The first 8 bytes of the payload is the instrument hash
                    long instrumentHash = bytes.readLong();

                    // The next part is the price string "1.0550", followed by '|'
                    // We can skip this for the risk calculation.
                    bytes.readSkip(7); // "1.0550" + "|"

                    // Next is the trade size
                    long tradeSize = bytes.parseLong();

                    processTick(instrumentHash, tradeSize);
                }
            }
        }
    }

    private static void processTick(long instrumentHash, long tradeSize) {
        searchKeyFlyweight.setValue(instrumentHash);
        try (var context = riskMap.acquireContext(searchKeyFlyweight, exposureFlyweight)) {
            long currentExposure = exposureFlyweight.getValue();
            long newExposure = currentExposure + tradeSize;
            exposureFlyweight.setValue(newExposure);
            System.out.println("Updated exposure for " + instrumentHash + " to " + newExposure);
        }
    }
}