package org.embulk.input.gcs;

import com.google.auth.Credentials;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.unit.LocalFile;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeNotNull;

public class TestAuthUtils
{
    private static Optional<String> GCP_EMAIL;
    private static Optional<String> GCP_P12_KEYFILE;
    private static Optional<String> GCP_JSON_KEYFILE;
    private static String GCP_BUCKET;
    private static final String GCP_APPLICATION_NAME = "embulk-input-gcs";

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
    }

    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private ConfigSource config;

    @Before
    public void setUp()
    {
        config = config();
    }

    @Test
    public void testGetServiceAccountCredentialSuccess() throws IOException, GeneralSecurityException
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        assertThat(AuthUtils.fromP12(task), instanceOf(Credentials.class));
    }

    @Test(expected = FileNotFoundException.class)
    public void testGetServiceAccountCredentialThrowFileNotFoundException()
            throws IOException, GeneralSecurityException
    {
        Path mockNotFound = Mockito.mock(Path.class);
        Mockito.when(mockNotFound.toString()).thenReturn("/path/to/notfound.p12");
        LocalFile p12File = Mockito.mock(LocalFile.class);
        Mockito.doReturn(mockNotFound).when(p12File).getPath();

        PluginTask task = Mockito.mock(PluginTask.class);
        Mockito.doReturn(Optional.of(p12File)).when(task).getP12Keyfile();
        AuthUtils.fromP12(task);
    }

    @Test
    public void testGetGcsClientUsingServiceAccountCredentialSuccess()
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        assertThat(AuthUtils.newClient(task), instanceOf(com.google.cloud.storage.Storage.class));
    }

    @Test(expected = ConfigException.class)
    public void testGetGcsClientUsingServiceAccountCredentialThrowJsonResponseException()
    {
        PluginTask task = config.set("bucket", "non-exists-bucket")
                .loadConfig(PluginTask.class);
        AuthUtils.newClient(task);
    }

    @Test
    public void testGetServiceAccountCredentialFromJsonFileSuccess()
            throws IOException
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        assertThat(AuthUtils.fromJson(task), instanceOf(Credentials.class));
    }

    @Test(expected = FileNotFoundException.class)
    public void testGetServiceAccountCredentialFromJsonThrowFileFileNotFoundException()
            throws IOException
    {
        Path mockNotFound = Mockito.mock(Path.class);
        Mockito.when(mockNotFound.toString()).thenReturn("/path/to/notfound.json");
        LocalFile jsonFile = Mockito.mock(LocalFile.class);
        Mockito.doReturn(mockNotFound).when(jsonFile).getPath();

        PluginTask task = Mockito.mock(PluginTask.class);
        Mockito.doReturn(Optional.of(jsonFile)).when(task).getJsonKeyfile();
        AuthUtils.fromJson(task);
    }

    @Test
    public void testGetServiceAccountCredentialFromJsonSuccess()
    {
        PluginTask task = config.set("auth_method", AuthUtils.AuthMethod.json_key).loadConfig(PluginTask.class);
        assertThat(AuthUtils.newClient(task), instanceOf(com.google.cloud.storage.Storage.class));
    }

    @Test(expected = ConfigException.class)
    public void testGetServiceAccountCredentialFromJsonThrowGoogleJsonResponseException()
    {
        PluginTask task = config.set("auth_method", AuthUtils.AuthMethod.json_key)
                .set("bucket", "non-exists-bucket")
                .loadConfig(PluginTask.class);
        AuthUtils.newClient(task);
    }

    private ConfigSource config()
    {
        return Exec.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", GCP_P12_KEYFILE)
                .set("json_keyfile", GCP_JSON_KEYFILE)
                .set("application_name", GCP_APPLICATION_NAME);
    }
}
