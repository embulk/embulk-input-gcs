package org.embulk.input.gcs;

import com.google.api.services.storage.Storage;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.util.Pages;
import org.embulk.standards.CsvParserPlugin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

import java.lang.reflect.InvocationTargetException;

import java.lang.reflect.Method;

public class TestGcsFileInputPlugin
{
    private static Optional<String> GCP_EMAIL;
    private static Optional<String> GCP_P12_KEYFILE;
    private static Optional<String> GCP_JSON_KEYFILE;
    private static String GCP_BUCKET;
    private static String GCP_BUCKET_DIRECTORY;
    private static String GCP_PATH_PREFIX;
    private static String GCP_APPLICATION_NAME;
    private static int MAX_CONNECTION_RETRY = 3;
    private FileInputRunner runner;
    private MockPageOutput output;

    /*
     * This test case requires environment variables
     *   GCP_EMAIL
     *   GCP_P12_KEYFILE
     *   GCP_JSON_KEYFILE
     *   GCP_BUCKET
     */
    @BeforeClass
    public static void initializeConstant()
    {
        String gcpEmail = System.getenv("GCP_EMAIL");
        String gcpP12KeyFile = System.getenv("GCP_P12_KEYFILE");
        String gcpJsonKeyFile = System.getenv("GCP_JSON_KEYFILE");
        String gcpBucket = System.getenv("GCP_BUCKET");

        // skip test cases, if environment variables are not set.
        assumeNotNull(gcpEmail, gcpP12KeyFile, gcpJsonKeyFile, gcpBucket);

        GCP_EMAIL = Optional.of(gcpEmail);
        GCP_P12_KEYFILE = Optional.of(gcpP12KeyFile);
        GCP_JSON_KEYFILE = Optional.of(gcpJsonKeyFile);
        GCP_BUCKET = gcpBucket;

        GCP_BUCKET_DIRECTORY = System.getenv("GCP_BUCKET_DIRECTORY") != null ? getDirectory(System.getenv("GCP_BUCKET_DIRECTORY")) : getDirectory("");
        GCP_PATH_PREFIX = GCP_BUCKET_DIRECTORY + "sample_";
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private ConfigSource config;
    private GcsFileInputPlugin plugin;

    @Before
    public void createResources() throws GeneralSecurityException, NoSuchMethodException, IOException
    {
        config = config();
        plugin = new GcsFileInputPlugin();
        runner = new FileInputRunner(runtime.getInstance(GcsFileInputPlugin.class));
        output = new MockPageOutput();
    }

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix");

        PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals(true, task.getIncremental());
        assertEquals("private_key", task.getAuthMethod().toString());
        assertEquals("Embulk GCS input plugin", task.getApplicationName());
    }

    // paths are set
    @Test
    public void checkDefaultValuesPathsSpecified()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("paths", Arrays.asList("object1", "object2"))
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", GCP_P12_KEYFILE)
                .set("p12_keyfile_fullpath", GCP_P12_KEYFILE)
                .set("parser", parserConfig(schemaConfig()));

        PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals(2, task.getPathFiles().size());
    }

    // both path_prefix and paths are not set
    @Test(expected = ConfigException.class)
    public void checkDefaultValuesNoPathSpecified()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", GCP_P12_KEYFILE)
                .set("p12_keyfile_fullpath", GCP_P12_KEYFILE)
                .set("parser", parserConfig(schemaConfig()));

        runner.transaction(config, new Control());
    }

    // p12_keyfile is null when auth_method is private_key
    @Test(expected = ConfigException.class)
    public void checkDefaultValuesP12keyNull()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", null)
                .set("parser", parserConfig(schemaConfig()));

        runner.transaction(config, new Control());
    }

    // both p12_keyfile and p12_keyfile_fullpath set
    @Test(expected = ConfigException.class)
    public void checkDefaultValuesConflictSetting()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", GCP_P12_KEYFILE)
                .set("p12_keyfile_fullpath", GCP_P12_KEYFILE)
                .set("parser", parserConfig(schemaConfig()));

        runner.transaction(config, new Control());
    }

    // invalid p12keyfile when auth_method is private_key
    @Test(expected = ConfigException.class)
    public void checkDefaultValuesInvalidPrivateKey()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", "invalid-key.p12")
                .set("parser", parserConfig(schemaConfig()));

        runner.transaction(config, new Control());
    }

    // json_keyfile is null when auth_method is json_key
    @Test(expected = ConfigException.class)
    public void checkDefaultValuesJsonKeyfileNull()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("auth_method", "json_key")
                .set("service_account_email", GCP_EMAIL)
                .set("json_keyfile", null)
                .set("parser", parserConfig(schemaConfig()));

        runner.transaction(config, new Control());
    }

    // last_path length is too long
    @Test(expected = ConfigException.class)
    public void checkDefaultValuesLongLastPath()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("auth_method", "json_key")
                .set("service_account_email", GCP_EMAIL)
                .set("json_keyfile", null)
                .set("last_path", "ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc128")
                .set("parser", parserConfig(schemaConfig()));

        runner.transaction(config, new Control());
    }

    @Test
    public void testGcsClientCreateSuccessfully()
            throws GeneralSecurityException, IOException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException
    {
        PluginTask task = config().loadConfig(PluginTask.class);
        runner.transaction(config, new Control());

        Method method = GcsFileInputPlugin.class.getDeclaredMethod("newGcsAuth", PluginTask.class);
        method.setAccessible(true);
        GcsFileInput.newGcsClient(task, (GcsAuthentication) method.invoke(plugin, task)); // no errors happens
    }

    @Test(expected = ConfigException.class)
    public void testGcsClientCreateThrowConfigException()
            throws GeneralSecurityException, IOException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException
    {
        ConfigSource config = Exec.newConfigSource()
                .set("bucket", "non-exists-bucket")
                .set("path_prefix", "my-prefix")
                .set("auth_method", "json_key")
                .set("service_account_email", GCP_EMAIL)
                .set("json_keyfile", GCP_JSON_KEYFILE)
                .set("parser", parserConfig(schemaConfig()));

        PluginTask task = config.loadConfig(PluginTask.class);
        runner.transaction(config, new Control());

        Method method = GcsFileInputPlugin.class.getDeclaredMethod("newGcsAuth", PluginTask.class);
        method.setAccessible(true);
        GcsFileInput.newGcsClient(task, (GcsAuthentication) method.invoke(plugin, task));
    }

    @Test
    public void testResume()
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        FileList.Builder builder = new FileList.Builder(config);
        builder.add("in/aa/a", 1);
        task.setFiles(builder.build());
        ConfigDiff configDiff = plugin.resume(task.dump(), 0, new FileInputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                return emptyTaskReports(taskCount);
            }
        });
        assertEquals("in/aa/a", configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testCleanup()
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.cleanup(task.dump(), 0, Lists.<TaskReport>newArrayList()); // no errors happens
    }

    @Test
    public void testListFilesByPrefix()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        List<String> expected = Arrays.asList(
                GCP_BUCKET_DIRECTORY + "sample_01.csv",
                GCP_BUCKET_DIRECTORY + "sample_02.csv"
        );

        PluginTask task = config.loadConfig(PluginTask.class);
        ConfigDiff configDiff = plugin.transaction(config, new FileInputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                assertEquals(2, taskCount);
                return emptyTaskReports(taskCount);
            }
        });

        Method method = GcsFileInputPlugin.class.getDeclaredMethod("newGcsAuth", PluginTask.class);
        method.setAccessible(true);
        Storage client = GcsFileInput.newGcsClient(task, (GcsAuthentication) method.invoke(plugin, task));
        FileList.Builder builder = new FileList.Builder(config);
        GcsFileInput.listGcsFilesByPrefix(builder, client, GCP_BUCKET, GCP_PATH_PREFIX, Optional.<String>absent());
        FileList fileList = builder.build();
        assertEquals(expected.get(0), fileList.get(0).get(0));
        assertEquals(expected.get(1), fileList.get(1).get(0));
        assertEquals(GCP_BUCKET_DIRECTORY + "sample_02.csv", configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testListFilesByPrefixWithPattern()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        List<String> expected = Arrays.asList(
                GCP_BUCKET_DIRECTORY + "sample_01.csv"
        );

        ConfigSource configWithPattern = config.deepCopy().set("path_match_pattern", "1");
        PluginTask task = configWithPattern.loadConfig(PluginTask.class);
        ConfigDiff configDiff = plugin.transaction(configWithPattern, new FileInputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                assertEquals(1, taskCount);
                return emptyTaskReports(taskCount);
            }
        });

        Method method = GcsFileInputPlugin.class.getDeclaredMethod("newGcsAuth", PluginTask.class);
        method.setAccessible(true);
        Storage client = GcsFileInput.newGcsClient(task, (GcsAuthentication) method.invoke(plugin, task));
        FileList.Builder builder = new FileList.Builder(configWithPattern);
        GcsFileInput.listGcsFilesByPrefix(builder, client, GCP_BUCKET, GCP_PATH_PREFIX, Optional.<String>absent());
        FileList fileList = builder.build();
        assertEquals(expected.get(0), fileList.get(0).get(0));
        assertEquals(GCP_BUCKET_DIRECTORY + "sample_01.csv", configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testListFilesByPrefixIncrementalFalse() throws Exception
    {
        ConfigSource config = config().deepCopy()
                .set("incremental", false);

        ConfigDiff configDiff = runner.transaction(config, new Control());

        assertEquals("{}", configDiff.toString());
    }

    @Test
    public void testListFilesByPrefixNonExistsBucket()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        runner.transaction(config, new Control());

        Method method = GcsFileInputPlugin.class.getDeclaredMethod("newGcsAuth", PluginTask.class);
        method.setAccessible(true);
        Storage client = GcsFileInput.newGcsClient(task, (GcsAuthentication) method.invoke(plugin, task));
        FileList.Builder builder = new FileList.Builder(config);
        GcsFileInput.listGcsFilesByPrefix(builder, client, "non-exists-bucket", "prefix", Optional.<String>absent()); // no errors happens
    }

    @Test
    public void testNonExistingFilesWithPathPrefix() throws Exception
    {
        ConfigSource config = Exec.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "/path/to/notfound")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", GCP_P12_KEYFILE)
                .set("json_keyfile", GCP_JSON_KEYFILE)
                .set("application_name", GCP_APPLICATION_NAME)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        ConfigDiff configDiff = runner.transaction(config, new Control());

        assertEquals("", configDiff.get(String.class, "last_path"));
    }

    @Test(expected = ConfigException.class)
    public void testNonExistingFilesWithPaths() throws Exception
    {
        ConfigSource config = Exec.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("paths", Arrays.asList())
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", GCP_P12_KEYFILE)
                .set("json_keyfile", GCP_JSON_KEYFILE)
                .set("application_name", GCP_APPLICATION_NAME)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        runner.transaction(config, new Control());
    }

    @Test
    public void testGcsFileInputByOpen()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException
    {
        ConfigSource config = Exec.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", GCP_PATH_PREFIX)
                .set("auth_method", "json_key")
                .set("service_account_email", GCP_EMAIL)
                .set("json_keyfile", GCP_JSON_KEYFILE)
                .set("parser", parserConfig(schemaConfig()));

        PluginTask task = config.loadConfig(PluginTask.class);
        runner.transaction(config, new Control());

        Method method = GcsFileInput.class.getDeclaredMethod("newGcsAuth", PluginTask.class);
        method.setAccessible(true);
        Storage client = GcsFileInput.newGcsClient(task, (GcsAuthentication) method.invoke(plugin, task));
        task.setFiles(GcsFileInput.listFiles(task, client));

        assertRecords(config, output);
    }

    @Test
    public void testBase64()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        Method method = GcsFileInput.class.getDeclaredMethod("base64Encode", String.class);
        method.setAccessible(true);

        assertEquals("CgFj", method.invoke(plugin, "c"));
        assertEquals("CgJjMg==", method.invoke(plugin, "c2"));
        assertEquals("Cgh0ZXN0LmNzdg==", method.invoke(plugin, "test.csv"));
        assertEquals("ChZnY3MtdGVzdC9zYW1wbGVfMDEuY3N2", method.invoke(plugin, "gcs-test/sample_01.csv"));
        String params = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc127";
        String expected = "Cn9jY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjMTI3";
        assertEquals(expected, method.invoke(plugin, params));
    }

    public ConfigSource config()
    {
        return Exec.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", GCP_PATH_PREFIX)
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", GCP_P12_KEYFILE)
                .set("json_keyfile", GCP_JSON_KEYFILE)
                .set("application_name", GCP_APPLICATION_NAME)
                .set("parser", parserConfig(schemaConfig()));
    }

    static List<TaskReport> emptyTaskReports(int taskCount)
    {
        ImmutableList.Builder<TaskReport> reports = new ImmutableList.Builder<>();
        for (int i = 0; i < taskCount; i++) {
            reports.add(Exec.newTaskReport());
        }
        return reports.build();
    }

    private class Control
            implements InputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource, Schema schema, int taskCount)
        {
            List<TaskReport> reports = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                reports.add(runner.run(taskSource, schema, i, output));
            }
            return reports;
        }
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    private ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "long"));
        builder.add(ImmutableMap.of("name", "time", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "comment", "type", "string"));
        return builder.build();
    }

    private void assertRecords(ConfigSource config, MockPageOutput output)
    {
        List<Object[]> records = getRecords(config, output);
        assertEquals(8, records.size());
        {
            Object[] record = records.get(0);
            assertEquals(1L, record[0]);
            assertEquals(32864L, record[1]);
            assertEquals("2015-01-27 19:23:49 UTC", record[2].toString());
            assertEquals("2015-01-27 00:00:00 UTC", record[3].toString());
            assertEquals("embulk", record[4]);
        }

        {
            Object[] record = records.get(1);
            assertEquals(2L, record[0]);
            assertEquals(14824L, record[1]);
            assertEquals("2015-01-27 19:01:23 UTC", record[2].toString());
            assertEquals("2015-01-27 00:00:00 UTC", record[3].toString());
            assertEquals("embulk jruby", record[4]);
        }
    }

    private List<Object[]> getRecords(ConfigSource config, MockPageOutput output)
    {
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        return Pages.toObjects(schema, output.pages);
    }

    private static String getDirectory(String dir)
    {
        if (dir != null && !dir.endsWith("/")) {
            dir = dir + "/";
        }
        if (dir.startsWith("/")) {
            dir = dir.replaceFirst("/", "");
        }
        return dir;
    }
}
