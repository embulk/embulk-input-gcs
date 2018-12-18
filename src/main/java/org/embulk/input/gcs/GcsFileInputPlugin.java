package org.embulk.input.gcs;

import com.google.cloud.storage.Storage;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;

import java.util.List;

public class GcsFileInputPlugin
        implements FileInputPlugin
{
    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // @see https://cloud.google.com/storage/docs/bucket-naming
        if (task.getLastPath().isPresent()) {
            final String lastPath = task.getLastPath().get();
            if (lastPath.length() < 3 || lastPath.length() >= 222) {
                throw new ConfigException("last_path length must be between 3 and 222");
            }
        }

        Storage client = ServiceUtils.newClient(task.getJsonKeyfile());

        // list files recursively if path_prefix is specified
        if (task.getPathPrefix().isPresent()) {
            task.setFiles(GcsFileInput.listFiles(task, client));
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
