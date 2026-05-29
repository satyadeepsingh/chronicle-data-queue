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

    // 1. THE ZERO-GC MAP DECLARATION
    // Notice we DO NOT use java.lang.Long for the value.
    // We use Chronicle's LongValue interface, which acts as a direct memory pointer.
    public static ChronicleMap<LongValue, LongValue> riskMap;


    private static final LongValue searchKeyFlyweight = Values.newNativeReference(LongValue.class);
    // 2. THE FLYWEIGHT VALUE LENS
    // This object is created ONCE at startup. It acts just like our Agrona UnsafeBuffer.
    // It has no memory of its own; it simply points to an address inside the Chronicle Map.
    private static final LongValue exposureFlyweight = Values.newNativeReference(LongValue.class);

    public static void main(String[] args) throws IOException {

        //Initialize an in memory memory-mapped state map
        riskMap = ChronicleMap.of(LongValue.class, LongValue.class)
                .name("fx-risk-map")
                .entries(10_000)
                .createPersistedTo(new File("ingress/map/fx-risk-map.dat"));

        try(ChronicleQueue ingressQueue = ChronicleQueue
                .singleBuilder("ingress/queue")
                .rollCycle(RollCycles.FAST_DAILY)
                .build()) {
            ExcerptTailer tailer = ingressQueue.createTailer();

            System.out.println("Starting pricing engine hot loop..");

            // 3. THE HOT LOOP (Pinned to an isolated core)
            while (true) {

                try(DocumentContext dc = tailer.readingDocument()) {
                    // If there is no new data, instantly release and loop again (Busy Spin)
                    if(!dc.isPresent()) {
                        continue;
                    }

                    //4. READ THE PRIMITIVES
                    Bytes<?> bytes = Objects.requireNonNull(dc.wire()).bytes();


                    //1. GET THE STARTING ADDRESS
                    // We find out exactly where this message starts in the memory mapped queue
                    long startPosition = bytes.readPosition();

                    // Find the packet length by end readLimit pointer
                    long packetLength = bytes.readLimit() - startPosition;

                    //2. READ THE HASH DIRECTLY FROM RAW ASCII BYTES
                    // The first 8 bytes of the raw network packet are the ticker string(e.g. "EUR/GBP|") 1 char = 1 byte
                    // so 8 bytes including the delimiter
                    // By calling the readLong(offset), we read those 8 ASCII bytes into a single 8 byte primitive long
                    long instrumentHash = bytes.readLong(startPosition);

                    //3. PARSE FIXED-POINT FROM RAW ASCII
                    // The price starts at byte 8. We pass the Chronicle Bytes object to our
                    // Zero-GC fixed-point parser
                    long bidPrice = parsePriceFixedPoint(bytes, (int)startPosition + 8);

                    long tradeSize = bytes.readLong();

                    // 4. SKIP THE RAW PAYLOAD
                    // Because we used absolute offsets to read the data, Chronicle's internal read cursor
                    // hasn't moved. We must manually skip past the network packet.
                    bytes.readSkip(packetLength);


                    // 5. READ THE METADATA
                    // Now the cursor is sitting exactly where the Gateway appended the timestamp.
                    long ingestionTimestamp = bytes.readLong();

                    // 6. EXECUTE
                    processTick(instrumentHash, bidPrice, tradeSize);
                }

            }
        }
    }

    private static void processTick(long instrumentHash, long bidPrice, long tradeSize) {

        //6. ZERO GC STATE MUTATION
        // We use acquireContext. This lock the specific segment of the map for thread safety.
        // finds the memory address for this instrumentHash, and points our flyweight directly at it.
        searchKeyFlyweight.setValue(instrumentHash);
        try(var context = riskMap.acquireContext(searchKeyFlyweight, exposureFlyweight)) {

            // Read the current exposure directly from off-heap /dev/shm memory
            long currentExposure = exposureFlyweight.getValue();

            // Calculate the new risk limit
            long newExposure = currentExposure + tradeSize;

            // Write the updated exposure directly back into the map's native memory.
            // Absolutely zero objects were boxed or allocated during this entire transaction.
            exposureFlyweight.setValue(newExposure);
        }

        // 7. THE ALGORITHMIC PRICING CALCULATION
        long finalBidPrice = bidPrice;
        long riskLimit = 1000_000L; // Example: 1 Million unit position limit

        // If our inventory of this asset is too high, we dynamically skew the spread
        if(exposureFlyweight.getValue() > riskLimit) {
            // Skew our bid down by 5 pips (represented as 500 in fixed-point math)
            // This discourages clients from selling us more of this asset.
            finalBidPrice -= 500;
        }

        // 8. EGRESS
        // At this point, we would use an ExcerptAppender to write finalPublishedBid
        // to our Egress Chronicle Queue, where Aeron will pick it up and multicast it to clients.
        System.out.println("Calculated new bid: " + finalBidPrice);


    }

    /**
     * Reads ASCII characters directly from memory and converts them to a fixed-point long.
     * E.g., reads bytes '1', '5', '0', '.', '2', '5', '|' and returns 150250L
     */
    private static long parsePriceFixedPoint(Bytes<?> bytes, int offset) {
        long price = 0;
        int currentOffset = offset;
        byte b;

        // Loop until we hit the pipe '|' delimiter (ASCII value 124)
        while ((b = bytes.readByte(currentOffset)) != '|') {
            if (b != '.') { // Ignore the decimal point; we are implicitly tracking the precision
                // Multiply the running total by 10 and add the numeric value of the ASCII byte
                price = (price * 10) + (b - '0');
            }
            currentOffset++;
        }

        return price;
    }


}
