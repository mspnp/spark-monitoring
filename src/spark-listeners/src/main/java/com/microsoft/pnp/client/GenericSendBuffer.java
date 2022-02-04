package com.microsoft.pnp.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public abstract class GenericSendBuffer<T> implements AutoCloseable {

    /**
     * This executor that will be shared among all buffers. We may not need this, since we
     * shouldn't have hundreds of different time generated fields, but we can't have
     * executors spinning up hundreds of threads.
     * <p>
     * We won't use daemon threads because in the event of a crash, we want to try to send
     * as much data as we can.
     */
    static ExecutorService executor = Executors.newCachedThreadPool();

    // Configure whether to throw exception on failed send for oversized event
    private static final boolean EXCEPTION_ON_FAILED_SEND = true;

    // Interface to support event notifications with a parameter.
    public interface Listener<T> {
        void invoke(T o);
    }

    // making it available to every thread to see if changes happen.
    // also this value will be set only when shutdown is called.
    public volatile boolean isClosed = false;


    /**
     * Object used to serialize sendRequest calls.
     */
    private final Object sendBufferLock = new Object();

    /**
     * Current batching task.
     * Synchronized by {@code sendBufferLock}.
     */
    private GenericSendBufferTask<T> sendBufferTask = null;

    /**
     * Permits controlling the number of in flight SendMessage batches.
     */
    private final Semaphore inflightBatches;

    // Make configurable
    private final int maxInflightBatches = 4;

    protected GenericSendBuffer() {
        this.inflightBatches = new Semaphore(this.maxInflightBatches);
    }

    protected abstract GenericSendBufferTask<T> createSendBufferTask();

    public void send(T data) {
        // if this buffer is closed , then no need to proceed.
        if (this.isClosed) {
            return;
        }

        try {
            synchronized (this.sendBufferLock) {
                if (this.sendBufferTask == null
                        || (!this.sendBufferTask.addEvent(data))) {
                    // We need a new task because one of the following is true:
                    // 1.  We don't have one yet (i.e. first message!)
                    // 2.  The task is full
                    // 3.  The task's timeout elapsed
                    GenericSendBufferTask<T> sendBufferTask = this.createSendBufferTask();
                    // Make sure we don't have too many in flight at once.
                    // This WILL block the calling code, but it's simpler than
                    // building a circular buffer, although we are sort of doing that. :)
                    // Not sure we need this yet!
                    this.inflightBatches.acquire();
                    this.sendBufferTask = sendBufferTask;

                    // Register a listener for the event signaling that the
                    // batch task has completed (successfully or not).
                    this.sendBufferTask.setOnCompleted(task -> {
                        inflightBatches.release();
                    });

                    // There is an edge case here.
                    // If the max bytes are too small for the first message, things go
                    // wonky, so let's bail
                    if (!this.sendBufferTask.addEvent(data)) {
                        String message = String.format("Failed to schedule batch because first message size %d exceeds batch size limit %d (bytes).",
                        this.sendBufferTask.calculateDataSize(data),
                        this.sendBufferTask.getMaxBatchSizeBytes());
                        System.err.println(message);
                        if(EXCEPTION_ON_FAILED_SEND) {
                            // If we are throwing before we call execute on the sendBufferTask, we should release the semaphore.
                            inflightBatches.release();
                            throw new RuntimeException(message);
                        }
                    }
                    executor.execute(this.sendBufferTask);
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            RuntimeException toThrow = new RuntimeException("Interrupted while waiting for lock.");
            toThrow.initCause(e);
            throw toThrow;
        }
    }

    /**
     * Flushes all outstanding outbound events in this buffer.
     * <p>
     * The call returns successfully when all outstanding events submitted before the
     * call are completed.
     */
    public void flush() {

        try {
            synchronized (sendBufferLock) {
                inflightBatches.acquire(this.maxInflightBatches);
                inflightBatches.release(this.maxInflightBatches);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    /**
     * makes this buffer closed,
     * flush what is in
     * executor will complete what is in and will not accept new entries.
     */
    public void close() {
        this.isClosed = true;
        flush();
        this.executor.shutdown();
    }


}

