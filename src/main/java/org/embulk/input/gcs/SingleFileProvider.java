package org.embulk.input.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import org.embulk.spi.Exec;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.ResumableInputStream;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Iterator;

import static org.embulk.input.gcs.RetryUtils.get;
import static org.embulk.input.gcs.RetryUtils.withRetry;

public class SingleFileProvider
        implements InputStreamFileInput.Provider
{
    private final Storage client;
    private final String bucket;
    private final Iterator<String> iterator;
    private boolean opened = false;
    private final RetryUtils.Task task;

    SingleFileProvider(PluginTask task, int taskIndex)
    {
        this.client = AuthUtils.newClient(task);
        this.bucket = task.getBucket();
        this.iterator = task.getFiles().get(taskIndex).iterator();
        this.task = task;
    }

    @Override
    public InputStream openNext()
    {
        if (opened) {
            return null;
        }
        opened = true;
        if (!iterator.hasNext()) {
            return null;
        }
        String key = iterator.next();
        ReadChannel ch = withRetry(task, get(client, bucket, key)).reader();
        ResumableInputStream.Reopener opener = new SeekableChannelReopener(client, bucket, key, task);
        return new ResumableInputStream(Channels.newInputStream(ch), opener);
    }

    @Override
    public void close()
    {
    }

    static class SeekableChannelReopener
            implements ResumableInputStream.Reopener
    {
        private final Logger log = Exec.getLogger(getClass());

        private final Storage client;
        private final String bucket;
        private final String key;
        private final RetryUtils.Task task;

        SeekableChannelReopener(Storage client, String bucket, String key, RetryUtils.Task task)
        {
            this.client = client;
            this.bucket = bucket;
            this.key = key;
            this.task = task;
        }

        @Override
        public InputStream reopen(long offset, Exception closedCause) throws IOException
        {
            log.warn("GCS read failed. Retrying GET request with {} bytes offset", offset, closedCause);
            ReadChannel ch = withRetry(task, get(client, bucket, key)).reader();
            ch.seek(offset);
            return Channels.newInputStream(ch);
        }
    }
}
