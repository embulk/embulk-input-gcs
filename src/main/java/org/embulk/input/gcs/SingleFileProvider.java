package org.embulk.input.gcs;

import com.google.api.client.util.IOUtils;
import com.google.api.services.storage.Storage;
import com.google.common.base.Throwables;
import org.embulk.spi.Exec;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
    private final Logger log = Exec.getLogger(SingleFileProvider.class);

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
        File tempFile = Exec.getTempFileSpace().createTempFile();
        getRemoteContentsWithRetry(tempFile, client, bucket, key, maxConnectionRetry);
        return new BufferedInputStream(new FileInputStream(tempFile));
    }

    @Override
    public void close()
    {
    }

    private Void getRemoteContentsWithRetry(final File tempFile, final Storage client, final String bucket, final String key, int maxConnectionRetry)
    {
        try {
            return retryExecutor()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWait(500)
                    .withMaxRetryWait(30 * 1000)
                    .runInterruptible(new RetryExecutor.Retryable<Void>() {
                        @Override
                        public Void call() throws IOException
                        {
                            Storage.Objects.Get getObject = client.objects().get(bucket, key);
                            try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(tempFile))) {
                                IOUtils.copy(getObject.executeMediaAsInputStream(), outputStream);
                            }
                            return null;
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
        catch (RetryExecutor.RetryGiveupException | InterruptedException ex) {
            throw Throwables.propagate(ex.getCause());
        }
    }
}
