package org.embulk.input.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Iterator;
import org.embulk.util.file.InputStreamFileInput;
import org.embulk.util.file.ResumableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleFileProvider implements InputStreamFileInput.Provider {
    private final Storage client;
    private final String bucket;
    private final Iterator<String> iterator;
    private boolean opened = false;

    SingleFileProvider(final PluginTask task, final int taskIndex) {
        this.client = AuthUtils.newClient(task);
        this.bucket = task.getBucket();
        this.iterator = task.getFiles().get(taskIndex).iterator();
    }

    @Override
    public InputStreamFileInput.InputStreamWithHints openNextWithHints() {
        if (opened) {
            return null;
        }
        opened = true;
        if (!iterator.hasNext()) {
            return null;
        }
        String key = iterator.next();
        ReadChannel ch = client.get(bucket, key).reader();
        return new InputStreamFileInput.InputStreamWithHints(
                new ResumableInputStream(Channels.newInputStream(ch), new InputStreamReopener(client, bucket, key)),
                String.format("gcs://%s/%s", bucket, key)
        );
    }

    @Override
    public void close() {
    }

    static class InputStreamReopener implements ResumableInputStream.Reopener {
        private Logger logger = LoggerFactory.getLogger(getClass());
        private final Storage client;
        private final String bucket;
        private final String key;

        InputStreamReopener(final Storage client, final String bucket, final String key) {
            this.client = client;
            this.bucket = bucket;
            this.key = key;
        }

        @Override
        public InputStream reopen(final long offset, final Exception closedCause) throws IOException {
            logger.warn(String.format("GCS read failed. Retrying GET request with %,d bytes offset", offset), closedCause);
            ReadChannel ch = client.get(bucket, key).reader();
            ch.seek(offset);
            return Channels.newInputStream(ch);
        }
    }
}
