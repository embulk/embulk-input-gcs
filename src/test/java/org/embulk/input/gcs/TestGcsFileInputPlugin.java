/*
 * Copyright 2015 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.input.gcs;

import static org.embulk.input.gcs.GcsFileInputPlugin.CONFIG_MAPPER;
import static org.embulk.input.gcs.GcsFileInputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.output.file.LocalFileOutputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.units.LocalFile;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class TestGcsFileInputPlugin {
    private static Optional<String> GCP_EMAIL;
    private static Optional<String> GCP_P12_KEYFILE;
    private static Optional<String> GCP_JSON_KEYFILE;
    private static String GCP_BUCKET;
    private static String GCP_BUCKET_DIRECTORY;
    private static String GCP_PATH_PREFIX;
    private static String GCP_APPLICATION_NAME = "embulk-input-gcs";
    private static final EmbulkSystemProperties EMBULK_SYSTEM_PROPERTIES;

    static {
        final Properties properties = new Properties();
        properties.setProperty("default_guess_plugins", "gzip,bzip2,json,csv");
        EMBULK_SYSTEM_PROPERTIES = EmbulkSystemProperties.of(properties);
    }

    /*
     * This test case requires environment variables
     *   GCP_EMAIL
     *   GCP_P12_KEYFILE
     *   GCP_JSON_KEYFILE
     *   GCP_BUCKET
     */
    @BeforeClass
    public static void initializeConstant() {
        String gcpEmail = System.getenv("GCP_EMAIL");
        String gcpP12KeyFile = System.getenv("GCP_PRIVATE_KEYFILE");
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
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .setEmbulkSystemProperties(EMBULK_SYSTEM_PROPERTIES)
            .registerPlugin(FormatterPlugin.class, "csv", CsvFormatterPlugin.class)
            .registerPlugin(FileInputPlugin.class, "gcs", GcsFileInputPlugin.class)
            .registerPlugin(FileOutputPlugin.class, "file", LocalFileOutputPlugin.class)
            .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
            .build();

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private ConfigSource config;
    private GcsFileInputPlugin plugin;

    @Before
    public void createResources() {
        config = config();
        plugin = new GcsFileInputPlugin();
    }

    @Test
    public void checkDefaultValues() {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix");

        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        assertTrue(task.getIncremental());
        assertEquals("private_key", task.getAuthMethod().toString());
        assertEquals("Embulk GCS input plugin", task.getApplicationName());
    }

    // paths are set
    @Test
    public void checkDefaultValuesPathsSpecified() {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("paths", Arrays.asList("object1", "object2"))
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("parser", parserConfig(schemaConfig()));
        setKeys(config);
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        assertEquals(2, task.getPathFiles().size());
    }

    // both path_prefix and paths are not set
    @Test(expected = ConfigException.class)
    public void checkDefaultValuesNoPathSpecified() {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", GCP_P12_KEYFILE)
                .set("p12_keyfile_fullpath", GCP_P12_KEYFILE)
                .set("parser", parserConfig(schemaConfig()));
        setKeys(config);
        plugin.transaction(config, new Control());
    }

    // p12_keyfile is null when auth_method is private_key
    @Test(expected = ConfigException.class)
    public void checkDefaultValuesP12keyNull() {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", null)
                .set("parser", parserConfig(schemaConfig()));
        plugin.transaction(config, new Control());
    }

    // both p12_keyfile and p12_keyfile_fullpath set
    @Test(expected = ConfigException.class)
    public void checkDefaultValuesConflictSetting() {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", Optional.of(LocalFile.ofContent("dummy")))
                .set("p12_keyfile_fullpath", Optional.of("dummy_path"))
                .set("parser", parserConfig(schemaConfig()));
        plugin.transaction(config, new Control());
    }

    // invalid p12keyfile when auth_method is private_key
    @Test(expected = ConfigException.class)
    public void checkDefaultValuesInvalidPrivateKey() {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", "invalid-key.p12")
                .set("parser", parserConfig(schemaConfig()));

        plugin.transaction(config, new Control());
    }

    // json_keyfile is null when auth_method is json_key
    @Test(expected = ConfigException.class)
    public void checkDefaultValuesJsonKeyfileNull() {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("auth_method", "json_key")
                .set("service_account_email", GCP_EMAIL)
                .set("json_keyfile", null)
                .set("parser", parserConfig(schemaConfig()));

        plugin.transaction(config, new Control());
    }

    // last_path length is too long
    @Test(expected = ConfigException.class)
    public void checkDefaultValuesLongLastPath() {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("auth_method", "json_key")
                .set("service_account_email", GCP_EMAIL)
                .set("json_keyfile", null)
                .set("last_path", "ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc128")
                .set("parser", parserConfig(schemaConfig()));

        plugin.transaction(config, new Control());
    }

    @Test
    public void testGcsClientCreateSuccessfully() {
        PluginTask task = CONFIG_MAPPER.map(config(), PluginTask.class);
        AuthUtils.newClient(task);
    }

    @Test(expected = ConfigException.class)
    public void testGcsClientCreateThrowConfigException() {
        // verify AuthUtils#newClient() to throws ConfigException for non-exists-bucket
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", "non-exists-bucket")
                .set("path_prefix", "my-prefix")
                .set("auth_method", "json_key")
                .set("service_account_email", GCP_EMAIL)
                .set("parser", parserConfig(schemaConfig()));
        setKeys(config);
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        AuthUtils.newClient(task);
    }

    @Test
    public void testResume() {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        FileList.Builder builder = new FileList.Builder(config);
        builder.add("in/aa/a", 1);
        task.setFiles(builder.build());
        ConfigDiff configDiff = plugin.resume(task.toTaskSource(), 0, (taskSource, taskCount) -> emptyTaskReports(taskCount));
        assertEquals("in/aa/a", configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testCleanup() {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        plugin.cleanup(task.toTaskSource(), 0, Lists.newArrayList()); // no errors happens
    }

    @Test
    public void testListFilesByPrefix() {
        List<String> expected = Arrays.asList(
                GCP_BUCKET_DIRECTORY + "sample_01.csv",
                GCP_BUCKET_DIRECTORY + "sample_02.csv"
        );

        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        ConfigDiff configDiff = plugin.transaction(config, (taskSource, taskCount) -> {
            assertEquals(2, taskCount);
            return emptyTaskReports(taskCount);
        });

        FileList fileList = GcsFileInput.listFiles(task);
        assertEquals(expected.get(0), fileList.get(0).get(0));
        assertEquals(expected.get(1), fileList.get(1).get(0));
        assertEquals(GCP_BUCKET_DIRECTORY + "sample_02.csv", configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testListFilesByPrefixWithPattern() {
        List<String> expected = Arrays.asList(
                GCP_BUCKET_DIRECTORY + "sample_01.csv"
        );

        ConfigSource configWithPattern = config.deepCopy().set("path_match_pattern", "1");
        PluginTask task = CONFIG_MAPPER.map(configWithPattern, PluginTask.class);
        ConfigDiff configDiff = plugin.transaction(configWithPattern, (taskSource, taskCount) -> {
            assertEquals(1, taskCount);
            return emptyTaskReports(taskCount);
        });

        FileList fileList = GcsFileInput.listFiles(task);
        assertEquals(expected.get(0), fileList.get(0).get(0));
        assertEquals(GCP_BUCKET_DIRECTORY + "sample_01.csv", configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testListFilesByPrefixIncrementalFalse() {
        ConfigSource config = config().deepCopy()
                .set("incremental", false);

        ConfigDiff configDiff = plugin.transaction(config, new Control());

        assertEquals("{}", configDiff.toString());
    }

    @Test(expected = ConfigException.class)
    public void testListFilesByPrefixNonExistsBucket() {
        PluginTask task = CONFIG_MAPPER.map(config
                .set("bucket", "non-exists-bucket")
                .set("path_prefix", "prefix"), PluginTask.class);
        plugin.transaction(config, new Control());

        // after refactoring, GcsFileInput#listFiles() won't accept initialized client
        // hence, this test will throw ConfigException
        GcsFileInput.listFiles(task);
    }

    @Test
    public void testNonExistingFilesWithPathPrefix() {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "/path/to/notfound")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("application_name", GCP_APPLICATION_NAME)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));
        setKeys(config);
        ConfigDiff configDiff = plugin.transaction(config, new Control());

        assertEquals("", configDiff.get(String.class, "last_path"));
    }

    @Test(expected = ConfigException.class)
    public void testNonExistingFilesWithPaths() throws Exception {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("paths", Arrays.asList())
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("application_name", GCP_APPLICATION_NAME)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));
        setKeys(config);
        plugin.transaction(config, new Control());
    }

    @Test(expected = ConfigException.class)
    public void testLastPathTooLong() throws Exception {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("paths", Arrays.asList())
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("application_name", GCP_APPLICATION_NAME)
                .set("last_path", "テストダミー/テストダミーテストダミーテストダミーテストダミーテストダミーテストダミーテストダミー.csv")
                .set("parser", parserConfig(schemaConfig()));
        setKeys(config);
        plugin.transaction(config, new Control());
    }

    @Test
    public void testGcsFileInputByOpen() throws IOException {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", GCP_PATH_PREFIX)
                .set("auth_method", "json_key")
                .set("service_account_email", GCP_EMAIL)
                .set("parser", parserConfig(schemaConfig()));
        setKeys(config);
        Path tempFile = embulk.createTempFile("csv");
        TestingEmbulk.RunResult result = embulk.runInput(config, tempFile);
        Schema schema = result.getInputSchema();
        assertEquals(schema.getColumns().size(), 5);
        assertRecords(tempFile);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Test
    public void testBase64() {
        assertEquals("CgFj", GcsFileInput.base64Encode("c"));
        assertEquals("CgJjMg==", GcsFileInput.base64Encode("c2"));
        assertEquals("Cgh0ZXN0LmNzdg==", GcsFileInput.base64Encode("test.csv"));
        assertEquals("ChZnY3MtdGVzdC9zYW1wbGVfMDEuY3N2", GcsFileInput.base64Encode("gcs-test/sample_01.csv"));
        String params = "ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc200";
        String expected = "CsgBY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2MyMDA=";
        assertEquals(expected, GcsFileInput.base64Encode(params));

        params = "テストダミー/テス123/テストダミー/テストダミ.csv";
        expected = "CkPjg4bjgrnjg4jjg4Djg5/jg7wv44OG44K5MTIzL+ODhuOCueODiOODgOODn+ODvC/jg4bjgrnjg4jjg4Djg58uY3N2";
        assertEquals(expected, GcsFileInput.base64Encode(params));
    }

    @Test
    public void testEncodeVarint() throws Exception {
        Method encodeVarintMethod = GcsFileInput.class.getDeclaredMethod("encodeVarint", int.class);
        encodeVarintMethod.setAccessible(true);

        byte[] expected1 = new byte[]{0x01};
        byte[] result1 = (byte[]) encodeVarintMethod.invoke(null, 1);
        assertArrayEquals("encodeVarint(1) should return {0x01}", expected1, result1);

        byte[] expected127 = new byte[]{0x7F};
        byte[] result127 = (byte[]) encodeVarintMethod.invoke(null, 127);
        assertArrayEquals("encodeVarint(127) should return {0x7F}", expected127, result127);

        byte[] expected128 = new byte[]{(byte) 0x80, 0x01};
        byte[] result128 = (byte[]) encodeVarintMethod.invoke(null, 128);
        assertArrayEquals("encodeVarint(128) should return {0x80, 0x01}", expected128, result128);

        byte[] expected1024 = new byte[]{(byte) 0x80, 0x08};
        byte[] result1024 = (byte[]) encodeVarintMethod.invoke(null, 1024);
        assertArrayEquals("encodeVarint(1024) should return {0x80, 0x08}", expected1024, result1024);
    }

    private ConfigSource config() {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", GCP_PATH_PREFIX)
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("application_name", GCP_APPLICATION_NAME)
                .set("parser", parserConfig(schemaConfig()));
        setKeys(config);
        return config;
    }

    private static List<TaskReport> emptyTaskReports(final int taskCount) {
        ImmutableList.Builder<TaskReport> reports = new ImmutableList.Builder<>();
        for (int i = 0; i < taskCount; i++) {
            reports.add(CONFIG_MAPPER_FACTORY.newTaskReport());
        }
        return reports.build();
    }

    private class Control implements FileInputPlugin.Control {
        @Override
        public List<TaskReport> run(TaskSource taskSource, int taskCount) {
            return ImmutableList.of(CONFIG_MAPPER_FACTORY.newTaskReport());
        }
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig) {
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

    private ImmutableList<Object> schemaConfig() {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "long"));
        builder.add(ImmutableMap.of("name", "time", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "comment", "type", "string"));
        return builder.build();
    }

    private void assertRecords(Path tempFile) throws IOException {
        InputStream in = new FileInputStream(tempFile.toFile());
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        List<String[]> records = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] record = line.split(",", 0);
            records.add(record);
        }
        assertEquals(8, records.size());

            {
                final String[] record = records.get(0);
                assertEquals("1", record[0]);
                assertEquals("32864", record[1]);
                assertEquals("2015-01-27 19:23:49.000000 +0000", record[2]);
                assertEquals("2015-01-27 00:00:00.000000 +0000", record[3]);
                assertEquals("embulk", record[4]);
            }

            {
                final Object[] record = records.get(1);
                assertEquals("2", record[0]);
                assertEquals("14824", record[1]);
                assertEquals("2015-01-27 19:01:23.000000 +0000", record[2].toString());
                assertEquals("2015-01-27 00:00:00.000000 +0000", record[3].toString());
                assertEquals("embulk jruby", record[4]);
            }
    }

    private static String getDirectory(String dir) {
        if (dir != null) {
            if (!dir.endsWith("/")) {
                dir = dir + "/";
            }
            if (dir.startsWith("/")) {
                dir = dir.replaceFirst("/", "");
            }
        }
        return dir;
    }

    private ConfigSource setKeys(final ConfigSource configSource) {
        byte[] keyBytes = Base64.getDecoder().decode(GCP_P12_KEYFILE.get());
        Optional<LocalFile> p12Key = Optional.of(LocalFile.ofContent(keyBytes));
        Optional<LocalFile> jsonKey = Optional.of(LocalFile.ofContent(GCP_JSON_KEYFILE.get().getBytes()));

        return configSource.set("p12_keyfile", p12Key)
                .set("json_keyfile", jsonKey);
    }
}
