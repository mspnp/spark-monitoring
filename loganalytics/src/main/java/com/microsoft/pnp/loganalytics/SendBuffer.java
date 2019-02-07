package com.microsoft.pnp.loganalytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendBuffer {

    private static final Logger logger = LoggerFactory.getLogger(SendBuffer.class);

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
    private interface Listener<T> {
        void invoke(T o);
    };

    private int maxBatchSizeBytes;

    private int maxBatchOpenMs;

    /**
     * The client to use for this buffer's operations.
     */
    private final LogAnalyticsClient client;

    /**
     * Object used to serialize sendRequest calls.
     */
    private final Object sendRequestLock = new Object();

    /**
     * Current batching task.
     * Synchronized by {@code sendRequestLock}.
     */
    private SendRequestTask sendRequestTask = null;

    /**
     * Permits controlling the number of in flight SendMessage batches.
     */
    private final Semaphore inflightBatches;

    // Make configurable
    private final int maxInflightBatches = 1;

    private final String timeGeneratedField;
    private final String logType;

    SendBuffer(LogAnalyticsClient client,
               String logType,
               String timeGenerateField,
               int maxMessageSizeInBytes,
               int batchTimeInMilliseconds
    ) {
        this.client = client;
        this.logType = logType;
        this.timeGeneratedField = timeGenerateField;
        this.maxBatchSizeBytes = maxMessageSizeInBytes;
        this.maxBatchOpenMs = batchTimeInMilliseconds;
        this.inflightBatches = new Semaphore(this.maxInflightBatches);
    }

    public void sendMessage(String message) {
        try {
            synchronized (this.sendRequestLock) {
                if (this.sendRequestTask == null
                        || (!this.sendRequestTask.addEvent(message))) {
                    logger.debug("Creating new task");
                    // We need a new task because one of the following is true:
                    // 1.  We don't have one yet (i.e. first message!)
                    // 2.  The task is full
                    // 3.  The task's timeout elapsed
                    SendRequestTask sendRequestTask = new SendRequestTask();
                    // Make sure we don't have too many in flight at once.
                    // This WILL block the calling code, but it's simpler than
                    // building a circular buffer, although we are sort of doing that. :)
                    // Not sure we need this yet!
                    logger.debug("Acquiring semaphore");
                    this.inflightBatches.acquire();
                    logger.debug("Acquired semaphore");
                    this.sendRequestTask = sendRequestTask;

                    // Register a listener for the event signaling that the
                    // batch task has completed (successfully or not).
                    this.sendRequestTask.setOnCompleted(task -> {
                        logger.debug("Releasing semaphore");
                        inflightBatches.release();
                        logger.debug("Released semaphore");
                    });

                    // There is an edge case here.
                    // If the max bytes are too small for the first message, things go
                    // wonky, so let's bail
                    if (!this.sendRequestTask.addEvent(message)) {
                        throw new RuntimeException("Failed to schedule batch");
                    }
                    executor.execute(this.sendRequestTask);
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
            synchronized (sendRequestLock) {
                inflightBatches.acquire(this.maxInflightBatches);
                inflightBatches.release(this.maxInflightBatches);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Task to send a batch of events to LogAnalytics.
     * <p>
     * The batch task is constructed open and accepts events until full, or until
     * {@code maxBatchOpenMs} elapses. At that point, the batch closes and the collected events
     * are assembled into a single request to LogAnalytics.
     * <p>
     * Instances of this class (and subclasses) are thread-safe.
     *
     */
    private class SendRequestTask implements Runnable {

        int batchSizeBytes = 0;
        protected final List<String> events;
        private boolean closed;
        private volatile Listener<SendRequestTask> onCompleted;

        public SendRequestTask() {
            this.events = new ArrayList<>();
        }

        public void setOnCompleted(Listener<SendRequestTask> value) {
            onCompleted = value;
        }

        /**
         * Adds an event to the batch if it is still open and has capacity.
         *
         * @param event
         * @return true if the event was added, otherwise, false
         */
        public synchronized boolean addEvent(String event) {
            if (closed) {
                return false;
            }

            boolean wasAdded = addIfAllowed(event);
            // If we can't add the event (because we are full), close the batch
            if (!wasAdded) {
                logger.debug("Could not add.  Closing batch");
                closed = true;
                notify();
            }

            return wasAdded;
        }

        /**
         * Adds the event to the batch if capacity allows it. Called by {@code addEvent} with a
         * lock on {@code this} held.
         *
         * @param event
         * @return true if it is okay to add the event, otherwise, false
         */
        private boolean addIfAllowed(String event) {

            if (isOkToAdd(event)) {
                logger.debug("Allowed to add");
                this.events.add(event);
                onEventAdded(event);
                return true;

            } else {
                return false;
            }
        }

        /**
         * Checks whether it's okay to add the event to this buffer. Called by
         * {@code addIfAllowed} with a lock on {@code this} held.
         *
         * @param event
         *            the event to add
         * @return true if the event is okay to add, otherwise, false
         */
        protected boolean isOkToAdd(String event) {
            return ((event.getBytes().length + batchSizeBytes) <= maxBatchSizeBytes);
        }

        /**
         * A hook to be run when an event is successfully added to this buffer. Called by
         * {@code addIfAllowed} with a lock on {@code this} held.
         *
         * @param event
         *            the event that was added
         */
        protected void onEventAdded(String event) {
            batchSizeBytes += event.getBytes().length;
        }

        /**
         * Processes the batch once closed. Is <em>NOT</em> called with a lock on {@code this}.
         * However, it's passed a local copy of the {@code event} list
         * made while holding the lock.
         */
        protected void process(List<String> events) {
            if (events.isEmpty()) {
                logger.debug("No events to send");
                return;
            }

            // Build up Log Analytics "batch" and send.
            // How should we handle failures?  I think there is retry built into the HttpClient,
            // but what if that fails as well?  I suspect we should just log it and move on.

            // We are going to assume that the events are properly formatted
            // JSON strings.  So for now, we are going to just wrap brackets around
            // them.
            StringBuffer sb = new StringBuffer("[");
            for (String event : events) {
                sb.append(event).append(",");
            }
            sb.deleteCharAt(sb.lastIndexOf(",")).append("]");
            try {
                logger.debug(sb.toString());
                client.send(sb.toString(), logType, timeGeneratedField);
            } catch (IOException ioe) {
                logger.error(ioe.getMessage(), ioe);
            }
        }

        @Override
        public final void run() {
            try {

                long deadlineMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS)
                        + maxBatchOpenMs + 1;
                long t = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);

                List<String> events;

                synchronized (this) {
                    while (!closed && (t < deadlineMs)) {
                        t = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);

                        // zero means "wait forever", can't have that.
                        long toWait = Math.max(1, deadlineMs - t);
                        wait(toWait);
                    }

                    closed = true;

                    events = new ArrayList<>(this.events);
                }

                logger.debug("Processing on thread " + Thread.currentThread().getName());
                process(events);
                logger.debug("Processing complete");
            } catch (InterruptedException e) {
                logger.error("run was interrupted", e);
                //throw e;
            } catch (RuntimeException e) {
                logger.error("RuntimeException", e);
                throw e;
            } catch (Error e) {
                Exception ex = new Exception("Error encountered", e);
                logger.error("Error encountered", ex);
                throw e;
            } finally {
                // make a copy of the listener since it (theoretically) can be
                // modified from the outside.
                Listener<SendRequestTask> listener = onCompleted;
                if (listener != null) {
                    logger.debug("Invoking completion callback.");
                    listener.invoke(this);
                }
            }
        }
    }
}
