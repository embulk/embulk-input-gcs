package org.embulk.input.gcs;

import com.google.common.base.Throwables;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.unit.LocalFile;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class GcsFileInputPlugin
        implements FileInputPlugin
{
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
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        if (AuthUtils.AuthMethod.json_key.equals(task.getAuthMethod())) {
            if (!task.getJsonKeyfile().isPresent()) {
                throw new ConfigException("If auth_method is json_key, you have to set json_keyfile");
            }
        }
        else if (AuthUtils.AuthMethod.private_key.equals(task.getAuthMethod())) {
            if (!task.getP12Keyfile().isPresent() || !task.getServiceAccountEmail().isPresent()) {
                throw new ConfigException("If auth_method is private_key, you have to set both service_account_email and p12_keyfile");
            }
        }

        // @see https://cloud.google.com/storage/docs/bucket-naming
        if (task.getLastPath().isPresent()) {
            if (task.getLastPath().get().length() >= 128) {
                throw new ConfigException("last_path length is allowed between 1 and 1024 bytes");
            }
        }

        // list files recursively if path_prefix is specified
        if (task.getPathPrefix().isPresent()) {
            task.setFiles(GcsFileInput.listFiles(task));
        }
        else {
            if (task.getPathFiles().isEmpty()) {
                throw new ConfigException("No file is found. Confirm paths option isn't empty");
            }
            FileList.Builder builder = new FileList.Builder(config);
            for (String file : task.getPathFiles()) {
                builder.add(file, 1);
            }
            task.setFiles(builder.build());
        }
        // number of processors is same with number of files
        return resume(task.dump(), task.getFiles().getTaskCount(), control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileInputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        control.run(taskSource, taskCount);

        ConfigDiff configDiff = Exec.newConfigDiff();

        if (task.getIncremental()) {
            configDiff.set("last_path", task.getFiles().getLastPath(task.getLastPath()));
        }

        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new GcsFileInput(task, taskIndex);
    }
}
