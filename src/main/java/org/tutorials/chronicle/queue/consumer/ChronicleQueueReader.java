package org.tutorials.chronicle.queue.consumer;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.DocumentContext;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class ChronicleQueueReader {

    public static void main(String[] args) throws IOException {

        try (ChronicleQueue ingressQueue = ChronicleQueue
                .singleBuilder("ingress/queue")
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
                }
            }
        }
    }

}