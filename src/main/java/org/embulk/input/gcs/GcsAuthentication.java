package org.embulk.input.gcs;

import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.compute.ComputeCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigException;
import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

import java.io.File;
import java.io.FileInputStream;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Optional;

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
        }
        else if (authMethod.toLowerCase().equals("json_key")) {
            this.credentials = getServiceAccountCredentialFromJsonFile();
        }
        else {
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
                .setServiceAccountId(serviceAccountEmail.orElseGet(null))
                .setServiceAccountScopes(
                        ImmutableList.of(
                                StorageScopes.DEVSTORAGE_READ_ONLY
                        )
                )
                .setServiceAccountPrivateKeyFromP12File(new File(p12KeyFilePath.get()))
                .build();
    }

    private GoogleCredential getServiceAccountCredentialFromJsonFile() throws IOException
    {
        FileInputStream stream = new FileInputStream(jsonKeyFilePath.get());

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

    public Storage getGcsClient(final String bucket, int maxConnectionRetry) throws ConfigException, IOException
    {
        try {
            return retryExecutor()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWait(500)
                    .withMaxRetryWait(30 * 1000)
                    .runInterruptible(new Retryable<Storage>() {
                        @Override
                        public Storage call() throws IOException, RetryGiveupException
                        {
                            Storage client = new Storage.Builder(httpTransport, jsonFactory, credentials)
                                    .setApplicationName(applicationName)
                                    .build();

                            // For throw ConfigException when authentication is fail.
                            long maxResults = 1;
                            client.objects().list(bucket).setMaxResults(maxResults).execute();

                            return client;
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            if (exception instanceof GoogleJsonResponseException) {
                                if (((GoogleJsonResponseException) exception).getDetails() == null) {
                                    if (((GoogleJsonResponseException) exception).getContent() != null) {
                                        String content = ((GoogleJsonResponseException) exception).getContent();
                                        log.warn("Invalid response was returned : {}", content);
                                        return true;
                                    }
                                }
                                int statusCode = ((GoogleJsonResponseException) exception).getDetails().getCode();
                                return !(statusCode / 100 == 4);
                            }
                            else if (exception instanceof TokenResponseException) {
                                TokenResponseException ex = (TokenResponseException) exception;
                                if (ex.getDetails() != null && ex.getDetails().getErrorDescription() != null) {
                                    String errorDescription = ex.getDetails().getErrorDescription();
                                    // Retry: 400 BadRequest "Invalid JWT..."
                                    // Caused by: com.google.api.client.auth.oauth2.TokenResponseException: 400 Bad Request
                                    // {
                                    //   "error" : "invalid_grant",
                                    //   "error_description" : "Invalid JWT: No valid verifier found for issuer."
                                    // }
                                    if (errorDescription.contains("Invalid JWT")) {
                                        log.warn("Invalid response was returned : {}", errorDescription);
                                        return true;
                                    }
                                }
                                return !(ex.getStatusCode() / 100 == 4);
                            }
                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                throws RetryGiveupException
                        {
                            String message = String.format("GCS GET request failed. Retrying %d/%d after %d seconds. Message: %s: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getClass(), exception.getMessage());
                            if (retryCount % 3 == 0) {
                                log.warn(message, exception);
                            }
                            else {
                                log.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException)
                                throws RetryGiveupException
                        {
                        }
                    });
        }
        catch (RetryGiveupException ex) {
            if (ex.getCause() instanceof GoogleJsonResponseException || ex.getCause() instanceof TokenResponseException) {
                int statusCode = 0;
                if (ex.getCause() instanceof GoogleJsonResponseException) {
                    if (((GoogleJsonResponseException) ex.getCause()).getDetails() != null) {
                        statusCode = ((GoogleJsonResponseException) ex.getCause()).getDetails().getCode();
                    }
                }
                else if (ex.getCause() instanceof TokenResponseException) {
                    statusCode = ((TokenResponseException) ex.getCause()).getStatusCode();
                }
                if (statusCode / 100 == 4) {
                    throw new ConfigException(ex);
                }
            }
            throw Throwables.propagate(ex);
        }
        catch (InterruptedException ex) {
            throw new InterruptedIOException();
        }
    }
}
