/*
 * Copyright 2018 The Embulk project
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import com.google.auth.Credentials;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Optional;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.util.config.units.LocalFile;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

public class TestAuthUtils {
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
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private ConfigSource config;

    @Before
    public void setUp() {
        config = config();
    }

    @Test
    public void testGetServiceAccountCredentialSuccess() throws IOException, GeneralSecurityException {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        assertTrue(AuthUtils.fromP12(task) instanceof Credentials);
    }

    @Test(expected = FileNotFoundException.class)
    public void testGetServiceAccountCredentialThrowFileNotFoundException() throws IOException, GeneralSecurityException {
        Path mockNotFound = Mockito.mock(Path.class);
        Mockito.when(mockNotFound.toString()).thenReturn("/path/to/notfound.p12");
        LocalFile p12File = Mockito.mock(LocalFile.class);
        Mockito.doReturn(mockNotFound).when(p12File).getPath();

        PluginTask task = Mockito.mock(PluginTask.class);
        Mockito.doReturn(Optional.of(p12File)).when(task).getP12Keyfile();
        AuthUtils.fromP12(task);
    }

    @Test
    public void testGetGcsClientUsingServiceAccountCredentialSuccess() {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        assertTrue(AuthUtils.newClient(task)  instanceof com.google.cloud.storage.Storage);
    }

    @Test(expected = ConfigException.class)
    public void testGetGcsClientUsingServiceAccountCredentialThrowJsonResponseException() {
        PluginTask task = CONFIG_MAPPER.map(config.set("bucket", "non-exists-bucket"), PluginTask.class);
        AuthUtils.newClient(task);
    }

    @Test
    public void testGetServiceAccountCredentialFromJsonFileSuccess() throws IOException {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        assertTrue(AuthUtils.fromJson(task) instanceof Credentials);
    }

    @Test(expected = FileNotFoundException.class)
    public void testGetServiceAccountCredentialFromJsonThrowFileFileNotFoundException() throws IOException {
        Path mockNotFound = Mockito.mock(Path.class);
        Mockito.when(mockNotFound.toString()).thenReturn("/path/to/notfound.json");
        LocalFile jsonFile = Mockito.mock(LocalFile.class);
        Mockito.doReturn(mockNotFound).when(jsonFile).getPath();

        PluginTask task = Mockito.mock(PluginTask.class);
        Mockito.doReturn(Optional.of(jsonFile)).when(task).getJsonKeyfile();
        AuthUtils.fromJson(task);
    }

    @Test
    public void testGetServiceAccountCredentialFromJsonSuccess() {
        PluginTask task = CONFIG_MAPPER.map(config.set("auth_method", AuthUtils.AuthMethod.json_key), PluginTask.class);
        assertTrue(AuthUtils.newClient(task)  instanceof com.google.cloud.storage.Storage);
    }

    @Test(expected = ConfigException.class)
    public void testGetServiceAccountCredentialFromJsonThrowGoogleJsonResponseException() {
        PluginTask task = CONFIG_MAPPER.map(config.set("auth_method", AuthUtils.AuthMethod.json_key)
                .set("bucket", "non-exists-bucket"), PluginTask.class);
        AuthUtils.newClient(task);
    }

    private ConfigSource config() {
        byte[] keyBytes = Base64.getDecoder().decode(GCP_P12_KEYFILE.get());
        Optional<LocalFile> p12Key = Optional.of(LocalFile.ofContent(keyBytes));
        Optional<LocalFile> jsonKey = Optional.of(LocalFile.ofContent(GCP_JSON_KEYFILE.get().getBytes()));

        return CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("bucket", GCP_BUCKET)
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", p12Key)
                .set("json_keyfile", jsonKey)
                .set("application_name", GCP_APPLICATION_NAME);
    }
}
