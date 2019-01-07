package org.embulk.input.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.slf4j.Logger;

public class GcsFileInput
        extends InputStreamFileInput
        implements TransactionalFileInput
{
    private static final Logger LOG = Exec.getLogger(org.embulk.input.gcs.GcsFileInput.class);

    GcsFileInput(PluginTask task, int taskIndex)
    {
        super(task.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
    }

    public void abort()
    {
    }

    public TaskReport commit()
    {
        return Exec.newTaskReport();
    }

    @Override
    public void close()
    {
    }

    /**
     * Lists GCS filenames filtered by prefix.
     *
     * The resulting list does not include the file that's size == 0.
     */
    static FileList listFiles(PluginTask task)
    {
        Storage client = AuthUtils.newClient(task);
        String bucket = task.getBucket();

        // @see https://cloud.google.com/storage/docs/json_api/v1/buckets/get
        if (LOG.isDebugEnabled()) {
            printBucketInfo(client, bucket);
        }

        String prefix = task.getPathPrefix().orElse("");
        String lastKey = task.getLastPath().isPresent() ? base64Encode(task.getLastPath().get()) : "";
        FileList.Builder builder = new FileList.Builder(task);

        try {
            // @see https://cloud.google.com/storage/docs/json_api/v1/objects/list
            Page<Blob> blobs = client.list(bucket, Storage.BlobListOption.prefix(prefix), Storage.BlobListOption.pageToken(lastKey));
            for (Blob blob : blobs.iterateAll()) {
                if (blob.getSize() > 0) {
                    builder.add(blob.getName(), blob.getSize());
                }
                LOG.debug("filename: {}", blob.getName());
                LOG.debug("updated: {}", blob.getUpdateTime());
            }
        }
        catch (RuntimeException e) {
            if ((e instanceof StorageException) && ((StorageException) e).getCode() == 400) {
                throw new ConfigException(String.format("Files listing failed: bucket:%s, prefix:%s, last_path:%s", bucket, prefix, lastKey), e);
            }

            LOG.warn(String.format("Could not get file list from bucket:%s", bucket));
            LOG.warn(e.getMessage());
        }
        return builder.build();
    }

    // String nextToken = base64Encode(0x0a + 0x01~0x27 + filePath);
    @VisibleForTesting
    static String base64Encode(String path)
    {
        byte[] encoding;
        byte[] utf8 = path.getBytes(Charsets.UTF_8);
        LOG.debug("path string: {} ,path length:{} \" + ", path, utf8.length);

        encoding = new byte[utf8.length + 2];
        encoding[0] = 0x0a;
        encoding[1] = Byte.valueOf(String.valueOf(path.length()));
        System.arraycopy(utf8, 0, encoding, 2, utf8.length);

        String s = BaseEncoding.base64().encode(encoding);
        LOG.debug("last_path(base64 encoded): {}", s);
        return s;
    }

    private static void printBucketInfo(Storage client, String bucket)
    {
        // get Bucket
        Storage.BucketGetOption fields = Storage.BucketGetOption.fields(
                Storage.BucketField.LOCATION,
                Storage.BucketField.TIME_CREATED,
                Storage.BucketField.OWNER
        );
        com.google.cloud.storage.Bucket bk = client.get(bucket, fields);
        LOG.debug("bucket name: {}", bk.getName());
        LOG.debug("bucket location: {}", bk.getLocation());
        LOG.debug("bucket timeCreated: {}", bk.getCreateTime());
        LOG.debug("bucket owner: {}", bk.getOwner());
    }
}
