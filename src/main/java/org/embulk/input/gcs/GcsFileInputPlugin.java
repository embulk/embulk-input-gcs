package org.embulk.input.gcs;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;

import com.google.api.client.http.HttpResponseException;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Optional;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import java.security.GeneralSecurityException;

import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.util.InputStreamFileInput;

import org.slf4j.Logger;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;

public class GcsFileInputPlugin
        implements FileInputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("bucket")
        String getBucket();

        @Config("path_prefix")
        String getPathPrefix();

        @Config("last_path")
        @ConfigDefault("null")
        Optional<String> getLastPath();

        @Config("auth_method")
        @ConfigDefault("\"private_key\"")
        AuthMethod getAuthMethod();

        @Config("service_account_email")
        @ConfigDefault("null")
        Optional<String> getServiceAccountEmail();

        @Config("application_name")
        @ConfigDefault("\"Embulk GCS input plugin\"")
        String getApplicationName();

        // kept for backward compatibility
        @Config("p12_keyfile_fullpath")
        @ConfigDefault("null")
        Optional<String> getP12KeyfileFullpath();

        @Config("p12_keyfile")
        @ConfigDefault("null")
        Optional<LocalFile> getP12Keyfile();
        void setP12Keyfile(Optional<LocalFile> p12Keyfile);

        @Config("json_keyfile")
        @ConfigDefault("null")
        Optional<LocalFile> getJsonKeyfile();

        List<String> getFiles();
        void setFiles(List<String> files);

        @ConfigInject
        BufferAllocator getBufferAllocator();
    }

    private static final Logger log = Exec.getLogger(GcsFileInputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        if (task.getP12KeyfileFullpath().isPresent()) {
            if (task.getP12Keyfile().isPresent()) {
                throw new ConfigException("Setting both p12_keyfile_fullpath and p12_keyfile is invalid");
            }
            try {
                task.setP12Keyfile(Optional.of(LocalFile.of(task.getP12KeyfileFullpath().get())));
            } catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        if (task.getAuthMethod().getString().equals("json_key")) {
            if (!task.getJsonKeyfile().isPresent()) {
                throw new ConfigException("If auth_method is json_key, you have to set json_keyfile");
            }
        } else if (task.getAuthMethod().getString().equals("private_key")) {
            if (!task.getP12Keyfile().isPresent() || !task.getServiceAccountEmail().isPresent()) {
                throw new ConfigException("If auth_method is private_key, you have to set both service_account_email and p12_keyfile");
            }
        }

        Storage client = newGcsClient(task, newGcsAuth(task));

        // list files recursively
        task.setFiles(listFiles(task, client));
        // number of processors is same with number of files
        return resume(task.dump(), task.getFiles().size(), control);
    }

    private GcsAuthentication newGcsAuth(PluginTask task)
    {
        try {
            return new GcsAuthentication(
                    task.getAuthMethod().getString(),
                    task.getServiceAccountEmail(),
                    task.getP12Keyfile().transform(localFileToPathString()),
                    task.getJsonKeyfile().transform(localFileToPathString()),
                    task.getApplicationName()
            );
        } catch (GeneralSecurityException | IOException ex) {
            throw new ConfigException(ex);
        }
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileInputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        control.run(taskSource, taskCount);

        ConfigDiff configDiff = Exec.newConfigDiff();

        List<String> files = new ArrayList<String>(task.getFiles());
        if (files.isEmpty()) {
            // keep the last value if any
            if (task.getLastPath().isPresent()) {
                configDiff.set("last_path", task.getLastPath().get());
            }
        } else {
            Collections.sort(files);
            configDiff.set("last_path", files.get(files.size() - 1));
        }

        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<TaskReport> successTaskReports)
    {
    }

    protected Storage newGcsClient(final PluginTask task, final GcsAuthentication auth)
    {
        Storage client = null;
        try {
            client = auth.getGcsClient(task.getBucket());
        } catch (IOException ex) {
            throw new ConfigException(ex);
        }

        return client;
    }

    private Function<LocalFile, String> localFileToPathString()
    {
        return new Function<LocalFile, String>()
        {
            public String apply(LocalFile file)
            {
                return file.getPath().toString();
            }
        };
    }

    public List<String> listFiles(PluginTask task, Storage client)
    {
        String bucket = task.getBucket();

        return listGcsFilesByPrefix(client, bucket, task.getPathPrefix(), task.getLastPath());
    }

    /**
     * Lists GCS filenames filtered by prefix.
     *
     * The resulting list does not include the file that's size == 0.
     */
    public static List<String> listGcsFilesByPrefix(Storage client, String bucket,
                                             String prefix, Optional<String> lastPath)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();

        String lastKey = lastPath.orNull();

        // @see https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        try {
            Storage.Buckets.Get getBucket = client.buckets().get(bucket);
            getBucket.setProjection("full");
            Bucket bk = getBucket.execute();

            log.debug("bucket name: " + bucket);
            log.debug("bucket location: " + bk.getLocation());
            log.debug("bucket timeCreated: " + bk.getTimeCreated());
            log.debug("bucket owner: " + bk.getOwner());
        } catch (IOException e) {
            log.warn("Could not access to bucket:" + bucket);
            log.warn(e.getMessage());
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
                        builder.add(o.getName());
                    }
                    log.debug("filename: " + o.getName());
                    log.debug("updated: " + o.getUpdated());
                }
                lastKey = objects.getNextPageToken();
                listObjects.setPageToken(lastKey);
            } while (lastKey != null);
        } catch (IOException e) {
            if ((e instanceof HttpResponseException) && ((HttpResponseException) e).getStatusCode() == 400) {
                throw new ConfigException(String.format("Files listing failed: bucket:%s, prefix:%s, last_path:%s", bucket, prefix, lastKey), e);
            }

            log.warn(String.format("Could not get file list from bucket:%s", bucket));
            log.warn(e.getMessage());
        }

        return builder.build();
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new GcsFileInput(task, taskIndex);
    }

    public class GcsFileInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {
        public GcsFileInput(PluginTask task, int taskIndex)
        {
            super(task.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
        }

        public void abort() { }

        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }

        @Override
        public void close() { }
    }

    private class SingleFileProvider
            implements InputStreamFileInput.Provider
    {
        private final Storage client;
        private final String bucket;
        private final String key;
        private boolean opened = false;

        public SingleFileProvider(PluginTask task, int taskIndex)
        {
            this.client = newGcsClient(task, newGcsAuth(task));
            this.bucket = task.getBucket();
            this.key = task.getFiles().get(taskIndex);
        }

        @Override
        public InputStream openNext() throws IOException
        {
            if (opened) {
                return null;
            }
            opened = true;
            Storage.Objects.Get getObject = client.objects().get(bucket, key);

            return getObject.executeMediaAsInputStream();
        }

        @Override
        public void close() { }
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
