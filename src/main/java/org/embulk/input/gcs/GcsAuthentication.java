package org.embulk.input.gcs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import java.security.GeneralSecurityException;
import java.util.Collections;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.compute.ComputeCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.Objects;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

public class GcsAuthentication
{
    private final Logger log = Exec.getLogger(GcsAuthentication.class);
    private final Optional<String> serviceAccountEmail;
    private final Optional<String> p12KeyFilePath;
    private final Optional<String> jsonKeyFilePath;
    private final String applicationName;
    private final HttpTransport httpTransport;
    private final JsonFactory jsonFactory;
    private final HttpRequestInitializer credentials;

    public GcsAuthentication(String authMethod, Optional<String> serviceAccountEmail,
            Optional<String> p12KeyFilePath, Optional<String> jsonKeyFilePath, String applicationName)
            throws IOException, GeneralSecurityException
    {
        this.serviceAccountEmail = serviceAccountEmail;
        this.p12KeyFilePath = p12KeyFilePath;
        this.jsonKeyFilePath = jsonKeyFilePath;
        this.applicationName = applicationName;

        this.httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        this.jsonFactory = new JacksonFactory();

        if (authMethod.equals("compute_engine")) {
            this.credentials = getComputeCredential();
        } else if(authMethod.toLowerCase().equals("json_key")) {
            this.credentials = getServiceAccountCredentialFromJsonFile();
        } else {
            this.credentials = getServiceAccountCredential();
        }
    }

    /**
     * @see https://developers.google.com/accounts/docs/OAuth2ServiceAccount#authorizingrequests
     */
    private GoogleCredential getServiceAccountCredential() throws IOException, GeneralSecurityException
    {
        // @see https://cloud.google.com/compute/docs/api/how-tos/authorization
        // @see https://developers.google.com/resources/api-libraries/documentation/storage/v1/java/latest/com/google/api/services/storage/STORAGE_SCOPE.html
        // @see https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/BigqueryScopes.html
        return new GoogleCredential.Builder()
                .setTransport(httpTransport)
                .setJsonFactory(jsonFactory)
                .setServiceAccountId(serviceAccountEmail.orNull())
                .setServiceAccountScopes(
                        ImmutableList.of(
                                StorageScopes.DEVSTORAGE_READ_ONLY
                        )
                )
                .setServiceAccountPrivateKeyFromP12File(new File(p12KeyFilePath.orNull()))
                .build();
    }

    private GoogleCredential getServiceAccountCredentialFromJsonFile() throws IOException
    {
        FileInputStream stream = new FileInputStream(jsonKeyFilePath.orNull());

        return GoogleCredential.fromStream(stream, httpTransport, jsonFactory)
                .createScoped(Collections.singleton(StorageScopes.DEVSTORAGE_READ_ONLY));
    }

    /**
     * @see http://developers.guge.io/accounts/docs/OAuth2ServiceAccount#creatinganaccount
     * @see https://developers.google.com/accounts/docs/OAuth2
     */
    private ComputeCredential getComputeCredential() throws IOException
    {
        ComputeCredential credential = new ComputeCredential.Builder(httpTransport, jsonFactory)
                .build();
        credential.refreshToken();

        return credential;
    }

    public Storage getGcsClient(String bucket) throws GoogleJsonResponseException, IOException
    {
        Storage client = new Storage.Builder(httpTransport, jsonFactory, credentials)
                .setApplicationName(applicationName)
                .build();

        // For throw IOException when authentication is fail.
        long maxResults = 1;
        Objects objects = client.objects().list(bucket).setMaxResults(maxResults).execute();

        return client;
    }
}