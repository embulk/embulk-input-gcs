package org.embulk.input.gcs;

import com.google.api.client.http.HttpResponseException;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.util.InputStreamFileInput;
import org.slf4j.Logger;

import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class GcsFileInput
        extends InputStreamFileInput
        implements TransactionalFileInput
{
    private static final Logger log = Exec.getLogger(org.embulk.input.gcs.GcsFileInput.class);

    public GcsFileInput(PluginTask task, int taskIndex)
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

    public static GcsAuthentication newGcsAuth(PluginTask task)
    {
        try {
            return new GcsAuthentication(
                    task.getAuthMethod().getString(),
                    task.getServiceAccountEmail(),
                    task.getP12Keyfile().map(localFileToPathString()),
                    task.getJsonKeyfile().map(localFileToPathString()),
                    task.getApplicationName()
            );
        }
        catch (GeneralSecurityException | IOException ex) {
            throw new ConfigException(ex);
        }
    }

    protected static Storage newGcsClient(final PluginTask task, final GcsAuthentication auth)
    {
        Storage client = null;
        try {
            client = auth.getGcsClient(task.getBucket(), task.getMaxConnectionRetry());
        }
        catch (IOException ex) {
            throw new ConfigException(ex);
        }

        return client;
    }

    private static Function<LocalFile, String> localFileToPathString()
    {
        return new Function<LocalFile, String>()
        {
            public String apply(LocalFile file)
            {
                return file.getPath().toString();
            }
        };
    }

    public static FileList listFiles(PluginTask task, Storage client)
    {
        String bucket = task.getBucket();

        FileList.Builder builder = new FileList.Builder(task);
        listGcsFilesByPrefix(builder, client, bucket, task.getPathPrefix().get(), task.getLastPath());
        return builder.build();
    }

    /**
     * Lists GCS filenames filtered by prefix.
     *
     * The resulting list does not include the file that's size == 0.
     */
    public static void listGcsFilesByPrefix(FileList.Builder builder, Storage client, String bucket,
                                             String prefix, Optional<String> lastPath)
    {
        String lastKey = lastPath.isPresent() ? base64Encode(lastPath.get()) : null;

        // @see https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        if (log.isDebugEnabled()) {
            try {
                Storage.Buckets.Get getBucket = client.buckets().get(bucket);
                getBucket.setProjection("full");
                Bucket bk = getBucket.execute();

                log.debug("bucket name: " + bucket);
                log.debug("bucket location: " + bk.getLocation());
                log.debug("bucket timeCreated: " + bk.getTimeCreated());
                log.debug("bucket owner: " + bk.getOwner());
            }
            catch (IOException e) {
                log.warn("Could not access to bucket:" + bucket);
                log.warn(e.getMessage());
            }
        }

        try {
            // @see https://cloud.google.com/storage/docs/json_api/v1/objects/list
            Storage.Objects.List listObjects = client.objects().list(bucket);
            listObjects.setPrefix(prefix);
            listObjects.setPageToken(lastKey);
            do {
                Objects objects = listObjects.execute();
                List<StorageObject> items = objects.getItems();
                if (items == null) {
                    log.info(String.format("No file was found in bucket:%s prefix:%s", bucket, prefix));
                    break;
                }
                for (StorageObject o : items) {
                    if (o.getSize().compareTo(BigInteger.ZERO) > 0) {
                        builder.add(o.getName(), o.getSize().longValue());
                    }
                    log.debug("filename: " + o.getName());
                    log.debug("updated: " + o.getUpdated());
                }
                lastKey = objects.getNextPageToken();
                listObjects.setPageToken(lastKey);
            } while (lastKey != null);
        }
        catch (IOException e) {
            if ((e instanceof HttpResponseException) && ((HttpResponseException) e).getStatusCode() == 400) {
                throw new ConfigException(String.format("Files listing failed: bucket:%s, prefix:%s, last_path:%s", bucket, prefix, lastKey), e);
            }

            log.warn(String.format("Could not get file list from bucket:%s", bucket));
            log.warn(e.getMessage());
        }
    }

    // String nextToken = base64Encode(0x0a + 0x01~0x27 + filePath);
    private static String base64Encode(String path)
    {
        byte[] encoding;
        byte[] utf8 = path.getBytes(Charsets.UTF_8);
        log.debug(String.format("path string: %s ,path length:%s \" + ", path, utf8.length));

        encoding = new byte[utf8.length + 2];
        encoding[0] = 0x0a;
        encoding[1] = new Byte(String.valueOf(path.length()));
        System.arraycopy(utf8, 0, encoding, 2, utf8.length);

        String s = BaseEncoding.base64().encode(encoding);
        log.debug(String.format("last_path(base64 encoded): %s", s));
        return s;
    }

    public enum AuthMethod
    {
        private_key("private_key"),
        compute_engine("compute_engine"),
        json_key("json_key");

        private final String string;

        AuthMethod(String string)
        {
            this.string = string;
        }

        public String getString()
        {
            return string;
        }
    }
}
