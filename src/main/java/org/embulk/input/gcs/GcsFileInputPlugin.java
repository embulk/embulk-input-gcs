package org.embulk.input.gcs;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.units.LocalFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class GcsFileInputPlugin
        implements FileInputPlugin
{
    public static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder()
            .addDefaultModules().build();
    public static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    public static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();
    private Logger logger = LoggerFactory.getLogger(GcsFileInputPlugin.class);
    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  FileInputPlugin.Control control)
    {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        if (task.getP12KeyfileFullpath().isPresent()) {
            if (task.getP12Keyfile().isPresent()) {
                throw new ConfigException("Setting both p12_keyfile_fullpath and p12_keyfile is invalid");
            }
            try {
                task.setP12Keyfile(Optional.of(LocalFile.of(task.getP12KeyfileFullpath().get())));
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
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
                throw new ConfigException("last_path length is allowed up to 127 characters");
            }
        }

        // list files recursively if path_prefix is specified
        if (task.getPathPrefix().isPresent()) {
            task.setFiles(GcsFileInput.listFiles(task));
            if (task.getFiles().getTaskCount() == 0) {
                logger.info("No file is found. Confirm path_prefix option is correct");
            }
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
        return resume(task.toTaskSource(), task.getFiles().getTaskCount(), control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileInputPlugin.Control control)
    {
        PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

        control.run(taskSource, taskCount);

        ConfigDiff configDiff = CONFIG_MAPPER_FACTORY.newConfigDiff();

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
        PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);
        return new GcsFileInput(task, taskIndex);
    }
}
