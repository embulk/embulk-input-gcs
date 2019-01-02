package org.embulk.input.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
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

    static FileList listFiles(PluginTask task, Storage client)
    {
        String bucket = task.getBucket();

        FileList.Builder builder = new FileList.Builder(task);
        listGcsFilesByPrefix(builder, client, bucket, task.getPathPrefix().orElse(null), task.getLastPath().orElse(null));
        return builder.build();
    }

    /**
     * Lists GCS filenames filtered by prefix.
     * <p>
     * The resulting list does not include the file that's size == 0.
     */
    static void listGcsFilesByPrefix(FileList.Builder builder, Storage client, String bucket,
                                     String prefix, String lastPath)
    {
        // @see https://cloud.google.com/storage/docs/json_api/v1/buckets/get
        if (LOG.isDebugEnabled()) {
            printBucketInfo(client, bucket);
        }

        // @see https://cloud.google.com/storage/docs/json_api/v1/objects/list
        Page<Blob> blobs = client.list(bucket, Storage.BlobListOption.prefix(prefix), Storage.BlobListOption.pageToken(lastPath));
        for (Blob blob : blobs.iterateAll()) {
            if (blob.getSize() > 0) {
                builder.add(blob.getName(), blob.getSize());
            }
            LOG.debug("filename: " + blob.getName());
            LOG.debug("updated: " + blob.getUpdateTime());
        }
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
        LOG.debug("bucket name: " + bk.getName());
        LOG.debug("bucket location: " + bk.getLocation());
        LOG.debug("bucket timeCreated: " + bk.getCreateTime());
        LOG.debug("bucket owner: " + bk.getOwner());
    }
}
