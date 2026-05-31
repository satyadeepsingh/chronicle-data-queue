package org.tutorials.chronicle;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Objects;



//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class UdpToChronicleIngestion {

    // 1. CHRONICLE QUEUE SETUP
    // Create the memory-mapped queue and acquire an appender to write data.
    private static final ChronicleQueue ingressQueue = ChronicleQueue
            .singleBuilder("ingress/queue")
            .build();

    private static final ExcerptAppender appender = ingressQueue.createAppender();

    // 2. PRE-ALLOCATED BUFFERS (Zero-GC)
    // We allocate a direct ByteBuffer once at startup. 2048 bytes easily holds a standard UDP frame.
    /**
     * Standard heap buffers (ByteBuffer.allocate()) require the JVM to copy the network data from the OS kernel space,
     * to native memory and then into the JVM heap.
     * A DirectByteBuffer asks the OS to write the network packet directly
     * into a native memory address that Java can access, eliminating a costly memory copy
     * and keeping the garbage collector completely blind to the data.
     */
    public static final ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

    /**
     * You might ask: If they(java.nio.ByteBuffer buffer above and the Agrona UnsafeBuffer below) both point to Address 0x1000, why not just use the standard ByteBuffer? Why introduce the Agrona library?
     * Because the standard Java ByteBuffer API is designed for safety, not speed.
     * Every time you call receiveBuffer.getLong(), the JVM performs bounds checking to ensure you aren't reading out of bounds. This costs CPU cycles.
     * Standard Java does not let you easily extract the raw memory address (which we needed for Chronicle).
     * Agrona's UnsafeBuffer is essentially a weaponized version of ByteBuffer. By wrapping the NIO buffer with Agrona, you unlock the ability to:
     * Bypass the JVM's bounds checking for pure hardware speed.
     * Call flyweight.addressOffset() to get the raw C-style memory pointer (Address 0x1000) so we can pass it directly to Chronicle's native copy method.
     */
    private static final UnsafeBuffer flyweight = new UnsafeBuffer(buffer);

    public static void main(String[] args) throws IOException {
        // 3. JAVA NIO UDP SETUP
        // Open a standard DatagramChannel and bind it to our multicast/unicast port.
        try(DatagramChannel channel = DatagramChannel.open()) {
            channel.bind(new InetSocketAddress(9999));

            /**
             * CRITICAL: Configure the channel to be non-blocking.
             * Our pinned thread will spin in a hot loop rather than sleeping to wait for data.
             * DatagramChannel.configureBlocking(false):
             * This is the secret to low-latency network I/O in standard Java.
             * Blocking I/O forces your thread to sleep until a packet arrives,
             * which causes a massive context-switch latency penalty when the OS wakes it up.
             * Non-blocking I/O allows your thread to hot-spin,
             * constantly polling the network card so it processes the packet the exact
             * nanosecond it drops into the buffer.
             */
            channel.configureBlocking(false);

            System.out.println("Listening for UDP packets on port 9999... ");

            // 4. THE HOT LOOP
            // This loop would run on an isolated, pinned CPU core.
            while (true) {

                // Clear the buffer's position/limit markers so it's ready to receive new data
                buffer.clear();

                // Attempt to read from the OS network stack into our off-heap buffer
                // Because it is non-blocking, this returns instantly (returns null if no packet is waiting)
                InetSocketAddress sender = (InetSocketAddress) channel.receive(buffer);

                if(sender != null) {
                    // A packet arrived!
                    // Flip the buffer so it's ready for reading instead of writing
                    buffer.flip();

                    int packetLength = buffer.remaining();
                    // 5. WRITE DIRECTLY TO CHRONICLE QUEUE
                    writeToQueue(packetLength);
                }
                // In a real HFT system, you would put an Agrona IdleStrategy here
                // (like BusySpinIdleStrategy) to prevent completely burning out the CPU if desired,
                // or simply let it hot-spin for absolute minimum latency.
            }

        }
    }

    private static void writeToQueue(int packetLength) {
        // writingDocument() provides a lock-free, zero-allocation context to write to the memory-mapped file
        try(DocumentContext dc = appender.writingDocument()) {
            Bytes<?> queueBytes = Objects.requireNonNull(dc.wire()).bytes();

            // PREPEND THE LENGTH OF THE PACKET BEFORE WRITING THE ACTUAL DATA
            queueBytes.writeInt(packetLength);

            // 1. GET SOURCE POINTER (Agrona)
            // Get the physical RAM address of our network buffer
            // get the raw C-style memory pointer (Address 0x1000) so we can pass it directly to Chronicle's native copy method.
            long srcAddress = flyweight.addressOffset();

            queueBytes.ensureCapacity(packetLength);

            // Get the exact physical RAM address where Chronicle is currently parked
            long destAddress = queueBytes.addressForWrite(queueBytes.writePosition());

            // 3. THE NATIVE COPY
            // OS.memory() is Chronicle's safe wrapper around sun.misc.Unsafe.
            // This executes a pure, CPU-level memory copy from the NIC buffer to
            OS.memory().copyMemory(srcAddress, destAddress, packetLength);

            // 4. ADVANCE THE CURSOR
            // Because we bypassed Chronicle's write methods, it doesn't know we added data.
            // We must manually advance the write cursor by the exact length of the packet.
            queueBytes.writeSkip(packetLength);

            // 5. APPEND METADATA
            // Now we can use standard Chronicle methods again to append our timestamp
            queueBytes.writeLong(System.nanoTime());
        }
    }
}