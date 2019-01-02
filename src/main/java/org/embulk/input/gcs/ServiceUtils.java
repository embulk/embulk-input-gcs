package org.embulk.input.gcs;

import com.google.api.client.auth.oauth2.TokenErrorResponse;
import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.embulk.config.ConfigException;
import org.embulk.spi.Exec;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.function.Predicate;

public class ServiceUtils
{
    private static final Logger LOG = Exec.getLogger(ServiceUtils.class);

    private static final Predicate<GoogleJsonResponseException> API_ERROR_NOT_RETRY_4XX = e -> {
        if (e.getDetails() == null && e.getContent() != null) {
            LOG.warn("Invalid response was returned : {}", e.getContent());
            return true;
        }
        int statusCode = e.getDetails().getCode();
        return statusCode / 100 != 4;
    };

    private static final Predicate<TokenResponseException> TOKEN_ERROR_NOT_RETRY_4XX = e -> {
        Optional<String> errDesc = Optional.ofNullable(e.getDetails()).map(TokenErrorResponse::getErrorDescription);
        if (errDesc.isPresent()) {
            // Retry: 400 BadRequest "Invalid JWT..."
            // Caused by: com.google.api.client.auth.oauth2.TokenResponseException: 400 Bad Request
            // {
            //   "error" : "invalid_grant",
            //   "error_description" : "Invalid JWT: No valid verifier found for issuer."
            // }
            if (errDesc.get().contains("Invalid JWT")) {
                LOG.warn("Invalid response was returned : {}", errDesc.get());
                return true;
            }
        }
        return e.getStatusCode() / 100 != 4;
    };

    public static abstract class DefaultRetryable<T> implements RetryExecutor.Retryable<T>
    {
        @Override
        public boolean isRetryableException(Exception exception)
        {
            if (exception instanceof GoogleJsonResponseException) {
                return API_ERROR_NOT_RETRY_4XX.test((GoogleJsonResponseException) exception);
            }
            else if (exception instanceof TokenResponseException) {
                return TOKEN_ERROR_NOT_RETRY_4XX.test((TokenResponseException) exception);
            }
            return true;
        }

        @Override
        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
        {
            String message = String.format("GCS GET request failed. Retrying %d/%d after %d seconds. Message: %s: %s",
                    retryCount, retryLimit, retryWait / 1000, exception.getClass(), exception.getMessage());
            if (retryCount % 3 == 0) {
                LOG.warn(message, exception);
            }
            else {
                LOG.warn(message);
            }
        }

        @Override
        public void onGiveup(Exception firstException, Exception lastException)
        {
        }
    }

    static Storage newClient(final Optional<LocalFile> jsonKeyfile)
    {
        if (jsonKeyfile.isPresent()) {
            try {
                final String path = jsonKeyfile.map(f -> f.getPath().toString()).get();
                final InputStream jsonStream = new FileInputStream(path);
                final Credentials credentials = ServiceAccountCredentials.fromStream(jsonStream);
                return StorageOptions.newBuilder()
                        .setCredentials(credentials)
                        .build()
                        .getService();
            }
            catch (IOException e) {
                throw new ConfigException(e);
            }
        }
        else {
            return StorageOptions.getDefaultInstance().getService();
        }
    }
}
