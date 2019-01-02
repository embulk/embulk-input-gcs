package org.embulk.input.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.InputStreamFileInput.InputStreamWithHints;
import org.embulk.spi.util.ResumableInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Iterator;

public class SingleFileProvider
        implements InputStreamFileInput.Provider
{
    private final Storage client;
    private final String bucket;
    private final Iterator<String> iterator;
    private boolean opened = false;

    SingleFileProvider(PluginTask task, int taskIndex)
    {
        this.client = ServiceUtils.newClient(task.getJsonKeyfile());
        this.bucket = task.getBucket();
        this.iterator = task.getFiles().get(taskIndex).iterator();
    }

    @Override
    public InputStreamWithHints openNextWithHints()
    {
        if (opened) {
            return null;
        }
        opened = true;
        if (!iterator.hasNext()) {
            return null;
        }
        String key = iterator.next();
        ReadChannel ch = client.get(bucket, key).reader();
        return new InputStreamWithHints(
                new ResumableInputStream(Channels.newInputStream(ch), new InputStreamReopener(client, bucket, key)),
                String.format("gcs://%s/%s", bucket, key)
        );
    }

    @Override
    public void close()
    {
    }

    static class InputStreamReopener
            implements ResumableInputStream.Reopener
    {
        private final Storage client;
        private final String bucket;
        private final String key;

        InputStreamReopener(Storage client, String bucket, String key)
        {
            this.client = client;
            this.bucket = bucket;
            this.key = key;
        }

        @Override
        public InputStream reopen(long offset, Exception closedCause) throws IOException
        {
            ReadChannel ch = client.get(bucket, key).reader();
            ch.seek(offset);
            return Channels.newInputStream(ch);
        }
    }
}
