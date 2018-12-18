package org.embulk.input.gcs;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.Task;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.unit.LocalFile;

import java.util.List;
import java.util.Optional;

public interface PluginTask
        extends Task, FileList.Task, RetryUtils.Task
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

    @Config("application_name")
    @ConfigDefault("\"Embulk GCS input plugin\"")
    String getApplicationName();

    @Config("json_keyfile")
    @ConfigDefault("null")
    Optional<LocalFile> getJsonKeyfile();

    @Config("paths")
    @ConfigDefault("[]")
    List<String> getPathFiles();

    FileList getFiles();
    void setFiles(FileList files);

    @ConfigInject
    BufferAllocator getBufferAllocator();
}
