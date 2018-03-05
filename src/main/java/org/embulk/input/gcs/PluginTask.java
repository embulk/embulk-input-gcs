package org.embulk.input.gcs;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.Task;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.unit.LocalFile;

import java.util.List;

public interface PluginTask
        extends Task, FileList.Task
{
    @Config("bucket")
    String getBucket();

    @Config("path_prefix")
    @ConfigDefault("null")
    Optional<String> getPathPrefix();

    @Config("last_path")
    @ConfigDefault("null")
    Optional<String> getLastPath();

    @Config("incremental")
    @ConfigDefault("true")
    boolean getIncremental();

    @Config("auth_method")
    @ConfigDefault("\"private_key\"")
    GcsFileInput.AuthMethod getAuthMethod();

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

    @Config("paths")
    @ConfigDefault("[]")
    List<String> getPathFiles();
    void setPathFiles(List<String> files);

    FileList getFiles();
    void setFiles(FileList files);

    @Config("max_connection_retry")
    @ConfigDefault("10") // 10 times retry to connect GCS server if failed.
    int getMaxConnectionRetry();

    @ConfigInject
    BufferAllocator getBufferAllocator();
}
