package org.apache.spark.listeners.microsoft.pnp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Task to send a batch of data.
 * <p>
 * The batch task is constructed open and accepts data until full, or until
 * {@code maxBatchOpenMs} elapses. At that point, the batch closes and the collected events
 * are assembled into a single request.
 * <p>
 * Instances of this class (and subclasses) are thread-safe.
 *
 */
public abstract class GenericSendBufferTask<T> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(GenericSendBufferTask.class);

    int currentBatchSize = 0;
    private final int maxBatchSizeBytes;
    private final int maxBatchOpenMs;
    private static final int DEFAULT_MAX_BATCH_OPEN_MILLISECONDS = 10000;

    protected final List<T> datas;
    private boolean closed;
    private volatile GenericSendBuffer.Listener<GenericSendBufferTask<T>> onCompleted;

    public GenericSendBufferTask(int maxBatchSizeBytes) {
        this(maxBatchSizeBytes, DEFAULT_MAX_BATCH_OPEN_MILLISECONDS);
    }

    public GenericSendBufferTask(int maxBatchSizeBytes, int maxBatchOpenMs) {
        this.datas = new ArrayList<>();
        this.maxBatchSizeBytes = maxBatchSizeBytes;
        this.maxBatchOpenMs = maxBatchOpenMs;
    }

    public void setOnCompleted(GenericSendBuffer.Listener<GenericSendBufferTask<T>> value) {
        this.onCompleted = value;
    }

    /**
     * Adds an event to the batch if it is still open and has capacity.
     *
     * @param data
     * @return true if the event was added, otherwise, false
     */
    public synchronized boolean addEvent(T data) {
        if (closed) {
            return false;
        }

        boolean wasAdded = addIfAllowed(data);
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
     * @param data
     * @return true if it is okay to add the event, otherwise, false
     */
    private boolean addIfAllowed(T data) {
        if (isOkToAdd(data)) {
            logger.debug("Allowed to add");
            this.datas.add(data);
            onEventAdded(data);
            return true;
        } else {
            return false;
        }
    }

    protected abstract int calculateDataSize(T data);

    /**
     * Checks whether it's okay to add the event to this buffer. Called by
     * {@code addIfAllowed} with a lock on {@code this} held.
     *
     * @param data
     *            the event to add
     * @return true if the event is okay to add, otherwise, false
     */
    protected boolean isOkToAdd(T data) {
        return ((this.calculateDataSize(data) + this.currentBatchSize) <= this.maxBatchSizeBytes);
    }

    /**
     * A hook to be run when an event is successfully added to this buffer. Called by
     * {@code addIfAllowed} with a lock on {@code this} held.
     *
     * @param data
     *            the event that was added
     */
    protected void onEventAdded(T data) {
        this.currentBatchSize += this.calculateDataSize(data);
    }

    /**
     * Processes the batch once closed. Is <em>NOT</em> called with a lock on {@code this}.
     * However, it's passed a local copy of the {@code event} list
     * made while holding the lock.
     */
    protected abstract void process(List<T> datas);

    @Override
    public final void run() {
        try {

            long deadlineMs = TimeUnit.MILLISECONDS.convert(
                    System.nanoTime(),
                    TimeUnit.NANOSECONDS
            ) + maxBatchOpenMs + 1;

            long t = TimeUnit.MILLISECONDS.convert(
                    System.nanoTime(),
                    TimeUnit.NANOSECONDS);

            List<T> datas;

            synchronized (this) {
                while (!closed && (t < deadlineMs)) {
                    t = TimeUnit.MILLISECONDS.convert(
                            System.nanoTime(),
                            TimeUnit.NANOSECONDS
                    );

                    // zero means "wait forever", can't have that.
                    long toWait = Math.max(1, deadlineMs - t);
                    wait(toWait);
                }

                closed = true;
                datas = new ArrayList<>(this.datas);
            }

            logger.debug("Processing on thread " + Thread.currentThread().getName());
            process(datas);
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
            GenericSendBuffer.Listener<GenericSendBufferTask<T>> listener = onCompleted;
            if (listener != null) {
                logger.debug("Invoking completion callback.");
                listener.invoke(this);
            }
        }
    }
}
