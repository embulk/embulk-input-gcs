package org.embulk.input.gcs;

import com.google.api.client.util.IOUtils;
import com.google.api.services.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.embulk.spi.Exec;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.ResumableInputStream;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.Iterator;

import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public class SingleFileProvider
        implements InputStreamFileInput.Provider
{
    private final Storage client;
    private final String bucket;
    private final Iterator<String> iterator;
    private final int maxConnectionRetry;
    private boolean opened = false;

    public SingleFileProvider(PluginTask task, int taskIndex)
    {
        this.client = GcsFileInput.newGcsClient(task, GcsFileInput.newGcsAuth(task));
        this.bucket = task.getBucket();
        this.iterator = task.getFiles().get(taskIndex).iterator();
        this.maxConnectionRetry = task.getMaxConnectionRetry();
    }

    @Override
    public InputStream openNext() throws IOException
    {
        if (opened) {
            return null;
        }
        opened = true;
        if (!iterator.hasNext()) {
            return null;
        }
        String key = iterator.next();
        Storage.Objects.Get getObject = client.objects().get(bucket, key);
        File tempFile = Exec.getTempFileSpace().createTempFile();
        try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            IOUtils.copy(getObject.executeMediaAsInputStream(), outputStream);
        }
        return new ResumableInputStream(new BufferedInputStream(new FileInputStream(tempFile)), new GcsInputStreamReopener(tempFile, client, bucket, key, maxConnectionRetry));
    }

    @Override
    public void close()
    {
    }

    @VisibleForTesting
    static class GcsInputStreamReopener
            implements ResumableInputStream.Reopener
    {
        private final Logger log = Exec.getLogger(GcsInputStreamReopener.class);
        private final File tempFile;
        private final Storage client;
        private final String bucket;
        private final String key;
        private final int maxConnectionRetry;

        public GcsInputStreamReopener(File tempFile, Storage client, String bucket, String key, int maxConnectionRetry)
        {
            this.tempFile = tempFile;
            this.client = client;
            this.bucket = bucket;
            this.key = key;
            this.maxConnectionRetry = maxConnectionRetry;
        }

        @Override
        public InputStream reopen(final long offset, final Exception closedCause) throws IOException
        {
            try {
                return retryExecutor()
                        .withRetryLimit(maxConnectionRetry)
                        .withInitialRetryWait(500)
                        .withMaxRetryWait(30 * 1000)
                        .runInterruptible(new RetryExecutor.Retryable<InputStream>() {
                            @Override
                            public InputStream call() throws IOException
                            {
                                log.warn(String.format("GCS read failed. Retrying GET request with %,d bytes offset", offset), closedCause);
                                Storage.Objects.Get getObject = client.objects().get(bucket, key);

                                try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))) {
                                    IOUtils.copy(getObject.executeMediaAsInputStream(), outputStream);
                                }
                                return new BufferedInputStream(new FileInputStream(tempFile));
                            }

                            @Override
                            public boolean isRetryableException(Exception exception)
                            {
                                return true;  // TODO
                            }

                            @Override
                            public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                    throws RetryExecutor.RetryGiveupException
                            {
                                String message = String.format("GCS GET request failed. Retrying %d/%d after %d seconds. Message: %s",
                                        retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                                if (retryCount % 3 == 0) {
                                    log.warn(message, exception);
                                }
                                else {
                                    log.warn(message);
                                }
                            }

                            @Override
                            public void onGiveup(Exception firstException, Exception lastException)
                                    throws RetryExecutor.RetryGiveupException
                            {
                            }
                        });
            }
            catch (RetryExecutor.RetryGiveupException ex) {
                Throwables.propagateIfInstanceOf(ex.getCause(), IOException.class);
                throw Throwables.propagate(ex.getCause());
            }
            catch (InterruptedException ex) {
                throw new InterruptedIOException();
            }
        }
    }
}
