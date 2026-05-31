package org.tutorials.chronicle.queue.consumer;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class ConcurrentRiskLimitValidator {

    private final ChronicleMap<LongValue, LongValue> riskMap;

    private final ThreadLocal<LongValue> threadLocalSearchKeyFlyweight = ThreadLocal.withInitial(
            () -> Values.newHeapInstance(LongValue.class));
    /**
     * Because LongValue is a mutable container, it is inherently thread-unsafe.
     * By wrapping it in ThreadLocal, you guarantee that
     * Thread A has its own isolated LongValue container,
     * and Thread B has a completely separate one.
     * Think of the two flyweights in terms of physical objects:
     * A Heap Flyweight (newHeapInstance) is a Bucket.
     * It physically exists inside the JVM heap and holds 8 bytes of actual memory.
     * A Native Flyweight (newNativeReference) is a Telescope.
     * It holds zero memory itself; it just points at memory that exists outside the JVM (in /dev/shm).
     * When you are updating an existing client's risk limit, that data already exists in /dev/shm.
     * If you aim the Native "Telescope" at it, you are looking directly at the live data.
     * If you used a Heap Flyweight for updates, your execution gate would be forced to do this:
     * Copy: Read the 8 bytes from /dev/shm and copy them into your JVM Heap Flyweight.
     * Mutate: Execute standard Java addition inside the JVM (heapFlyweight.setValue(current + trade)).
     * Copy: Write the 8 bytes from the JVM Heap Flyweight back into the /dev/shm memory block.
     * Moving data across the JVM boundary into the OS-level memory map takes precious CPU cycles.
     * Doing it twice per trade (Read -> Write) adds severe latency jitter.
     */
    private final ThreadLocal<LongValue> threadLocalExposureLens = ThreadLocal.withInitial(
            () -> Values.newNativeReference(LongValue.class));

    /**
     * When a completely new instrument arrives, there is no existing data in /dev/shm to point a telescope at.
     * The JVM must carry the initial trade exposure across the boundary into the newly allocated off-heap segment. That is why we use the Heap Flyweight (the Bucket) to carry those first 8 bytes across.
     * The Rule of Thumb for Zero-GC Memory Architecture:
     * To transport new data from the JVM into off-heap memory, use a reusable Heap Flyweight.
     * To mutate data that already lives in off-heap memory, use a reusable Native Flyweight to modify it in-place.
     */
    private final ThreadLocal<LongValue> threadLocalInsertionValueFlyweight = ThreadLocal.withInitial(
            () -> Values.newHeapInstance(LongValue.class));

    public ConcurrentRiskLimitValidator() throws IOException {
        this.riskMap = ChronicleMap.of(LongValue.class, LongValue.class)
                .entries(10_000)
                .createPersistedTo(new File("ingress/map/fx-risk-map.dat"));
    }

    public static long packInstrument(CharSequence s) {
        long result = 0;
        int len = Math.min(s.length(), 8);
        for(int i = 0; i < 8; i++) {
            // 1. (long) cast prevents the 32-bit shift truncation trap.
            // 2. & 0xFF strips any unexpected high bits, ensuring pure 8-bit ASCII.
            // 3. << (56 - (i * 8)) slots it perfectly into place.
            long l = i < len ? (byte) s.charAt(i) : (byte) ' ';
            result |= (l & 0xFF) << 56 - (i * 8);
        }

        return result;
    }

    public boolean validateRiskLimit(long instrumentHash, long tradeSize, long maxLimit) {

        // 2. Reuse the thread-local key flyweight (zero allocation)
        LongValue keyFlyweight = threadLocalSearchKeyFlyweight.get();
        keyFlyweight.setValue(instrumentHash);

        // 3. Acquire off-heap segment spinlock using the flyweight key
        try (ExternalMapQueryContext<LongValue, LongValue, ?> context = riskMap.queryContext(keyFlyweight)) {

            context.updateLock().lock();
            MapEntry<LongValue, LongValue> entry = context.entry();

            if(entry != null) {
                //4. Point our Value lens at the raw memory address
               var offHeapData = entry.value();

               LongValue exposureLens = threadLocalExposureLens.get();

                Byteable byteableLens = (Byteable) exposureLens;

                // 3. Aim the lens at the exact physical memory coordinates
                byteableLens.bytesStore((BytesStore) offHeapData.bytes(), offHeapData.offset(), offHeapData.size());

                // 4. Now the LongValue methods will execute against the physical RAM
                long currentExposure = exposureLens.getValue();

                if((currentExposure + tradeSize) <= maxLimit) {

                    /**
                     * The single biggest advantage of the Native Flyweight is the addAtomicValue() method.
                     * Because the Native Flyweight points directly to the raw, off-heap physical memory address,
                     * calling addAtomicValue() translates almost directly to a CPU-level Fetch-and-Add (XADD)
                     * or Compare-And-Swap (CAS) hardware instruction on that exact memory block. It modifies the memory in-place.
                     * If you use a Heap Flyweight (the Bucket), you cannot use bytesStore() to point it at /dev/shm.
                     * You are forced to execute a completely different, much slower pipeline.
                     */
                    exposureLens.addAtomicValue(tradeSize);
                    return true;
                } else return false;
            }
            return initializeAndValidateNewInstrument(context, tradeSize, maxLimit);
        }



    }

    private boolean initializeAndValidateNewInstrument(ExternalMapQueryContext<LongValue, LongValue, ?> context, long tradeSize, long maxLimit) {
        if(tradeSize <= maxLimit) {

            LongValue insertionFlyweight = threadLocalInsertionValueFlyweight.get();
            insertionFlyweight.setValue(tradeSize);
            // 3. Chronicle allocates the off-heap space, reads the 8 bytes from our flyweight,
            // and copies them into the new /dev/shm segment.
            // We do not need to aim the native lens here, because the value is already safely
            // written to off-heap memory by the insert() command.
            context.insert(Objects.requireNonNull(context.absentEntry()), context.wrapValueAsData(insertionFlyweight));
            return true;
        }

        return false;
    }
}
