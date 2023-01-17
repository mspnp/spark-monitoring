package com.microsoft.pnp.client;

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
     * Returns the max batch size for the implementation
     *
     * @return maximum batch size
     */
    protected int getMaxBatchSizeBytes() {
        return (maxBatchSizeBytes);
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

            process(datas);
        } catch (InterruptedException e) {
            // If the thread is interrupted, we should make a best effort to deliver the messages in the buffer.
            // This may result in duplicated messages if the thread is interrupted late in the execution of process(...)
            // but this is better than missing messages that might have information on an important error.
            process(new ArrayList<>(this.datas));
            this.datas.clear();
        } catch (RuntimeException e) {
            throw e;
        } catch (Error e) {
            throw new RuntimeException("Error encountered", e);
        } finally {
            // make a copy of the listener since it (theoretically) can be
            // modified from the outside.
            GenericSendBuffer.Listener<GenericSendBufferTask<T>> listener = onCompleted;
            if (listener != null) {
                listener.invoke(this);
            }
        }
    }
}
