package org.apache.spark.listeners.microsoft.pnp;

import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class GenericSendBuffer<T> {

    private static final Logger logger = LoggerFactory.getLogger(GenericSendBuffer.class);

    /**
     * This executor that will be shared among all buffers. We may not need this, since we
     * shouldn't have hundreds of different time generated fields, but we can't have
     * executors spinning up hundreds of threads.
     *
     * We won't use daemon threads because in the event of a crash, we want to try to send
     * as much data as we can.
     */
    static ExecutorService executor = Executors.newCachedThreadPool();

    // Interface to support event notifications with a parameter.
    public interface Listener<T> {
        void invoke(T o);
    }

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
    private final int maxInflightBatches = 1;

    protected GenericSendBuffer() {
        this.inflightBatches = new Semaphore(this.maxInflightBatches);
    }

    protected abstract GenericSendBufferTask<T> createSendBufferTask();

    public void send(T data) {
        try {
            synchronized (this.sendBufferLock) {
                if (this.sendBufferTask == null
                        || (!this.sendBufferTask.addEvent(data))) {
                    logger.debug("Creating new task");
                    // We need a new task because one of the following is true:
                    // 1.  We don't have one yet (i.e. first message!)
                    // 2.  The task is full
                    // 3.  The task's timeout elapsed
                    GenericSendBufferTask<T> sendBufferTask = this.createSendBufferTask();
                    // Make sure we don't have too many in flight at once.
                    // This WILL block the calling code, but it's simpler than
                    // building a circular buffer, although we are sort of doing that. :)
                    // Not sure we need this yet!
                    logger.debug("Acquiring semaphore");
                    this.inflightBatches.acquire();
                    logger.debug("Acquired semaphore");
                    this.sendBufferTask = sendBufferTask;

                    // Register a listener for the event signaling that the
                    // batch task has completed (successfully or not).
                    this.sendBufferTask.setOnCompleted(task -> {
                        logger.debug("Releasing semaphore");
                        inflightBatches.release();
                        logger.debug("Released semaphore");
                    });

                    // There is an edge case here.
                    // If the max bytes are too small for the first message, things go
                    // wonky, so let's bail
                    if (!this.sendBufferTask.addEvent(data)) {
                        throw new RuntimeException("Failed to schedule batch");
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
}

