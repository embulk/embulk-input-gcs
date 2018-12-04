package org.embulk.input.gcs;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.storage.Storage;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.FileNotFoundException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.GeneralSecurityException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

public class TestGcsAuthentication
{
    private static Optional<String> GCP_EMAIL;
    private static Optional<String> GCP_P12_KEYFILE;
    private static Optional<String> GCP_JSON_KEYFILE;
    private static String GCP_BUCKET;
    private static final String GCP_APPLICATION_NAME = "embulk-input-gcs";
    private static int MAX_CONNECTION_RETRY = 3;

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

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testGetServiceAccountCredentialSuccess()
            throws NoSuchFieldException, IllegalAccessException, GeneralSecurityException, IOException
    {
        GcsAuthentication auth = new GcsAuthentication(
                "private_key",
                GCP_EMAIL,
                GCP_P12_KEYFILE,
                null,
                GCP_APPLICATION_NAME
        );

        Field field = GcsAuthentication.class.getDeclaredField("credentials");
        field.setAccessible(true);

        assertEquals(GoogleCredential.class, field.get(auth).getClass());
    }

    @Test(expected = FileNotFoundException.class)
    public void testGetServiceAccountCredentialThrowFileNotFoundException()
            throws GeneralSecurityException, IOException
    {
        Optional<String> notFoundP12Keyfile = Optional.of("/path/to/notfound.p12");
        GcsAuthentication auth = new GcsAuthentication(
                "private_key",
                GCP_EMAIL,
                notFoundP12Keyfile,
                null,
                GCP_APPLICATION_NAME
        );
    }

    @Test
    public void testGetGcsClientUsingServiceAccountCredentialSuccess()
            throws NoSuchFieldException, IllegalAccessException, GeneralSecurityException, IOException
    {
        GcsAuthentication auth = new GcsAuthentication(
                "private_key",
                GCP_EMAIL,
                GCP_P12_KEYFILE,
                null,
                GCP_APPLICATION_NAME
        );

        Storage client = auth.getGcsClient(GCP_BUCKET, MAX_CONNECTION_RETRY);

        assertEquals(Storage.class, client.getClass());
    }

    @Test(expected = ConfigException.class)
    public void testGetGcsClientUsingServiceAccountCredentialThrowJsonResponseException()
            throws NoSuchFieldException, IllegalAccessException, GeneralSecurityException, IOException
    {
        GcsAuthentication auth = new GcsAuthentication(
                "private_key",
                GCP_EMAIL,
                GCP_P12_KEYFILE,
                null,
                GCP_APPLICATION_NAME
        );

        Storage client = auth.getGcsClient("non-exists-bucket", MAX_CONNECTION_RETRY);

        assertEquals(Storage.class, client.getClass());
    }

    @Test
    public void testGetServiceAccountCredentialFromJsonFileSuccess()
            throws NoSuchFieldException, IllegalAccessException, GeneralSecurityException, IOException
    {
        GcsAuthentication auth = new GcsAuthentication(
                "json_key",
                GCP_EMAIL,
                null,
                GCP_JSON_KEYFILE,
                GCP_APPLICATION_NAME
        );
        Field field = GcsAuthentication.class.getDeclaredField("credentials");
        field.setAccessible(true);

        assertEquals(GoogleCredential.class, field.get(auth).getClass());
    }

    @Test(expected = FileNotFoundException.class)
    public void testGetServiceAccountCredentialFromJsonThrowFileFileNotFoundException()
            throws GeneralSecurityException, IOException
    {
        Optional<String> notFoundJsonKeyfile = Optional.of("/path/to/notfound.json");
        GcsAuthentication auth = new GcsAuthentication(
                "json_key",
                GCP_EMAIL,
                null,
                notFoundJsonKeyfile,
                GCP_APPLICATION_NAME
        );
    }

    @Test
    public void testGetServiceAccountCredentialFromJsonSuccess()
            throws NoSuchFieldException, IllegalAccessException, GeneralSecurityException, IOException
    {
        GcsAuthentication auth = new GcsAuthentication(
                "json_key",
                GCP_EMAIL,
                null,
                GCP_JSON_KEYFILE,
                GCP_APPLICATION_NAME
        );

        Storage client = auth.getGcsClient(GCP_BUCKET, MAX_CONNECTION_RETRY);

        assertEquals(Storage.class, client.getClass());
    }

    @Test(expected = ConfigException.class)
    public void testGetServiceAccountCredentialFromJsonThrowGoogleJsonResponseException()
            throws NoSuchFieldException, IllegalAccessException, GeneralSecurityException, IOException
    {
        GcsAuthentication auth = new GcsAuthentication(
                "json_key",
                GCP_EMAIL,
                null,
                GCP_JSON_KEYFILE,
                GCP_APPLICATION_NAME
        );

        Storage client = auth.getGcsClient("non-exists-bucket", MAX_CONNECTION_RETRY);
    }
}
