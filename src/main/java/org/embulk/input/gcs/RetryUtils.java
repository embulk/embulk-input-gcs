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

import com.google.api.client.auth.oauth2.TokenErrorResponse;
import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import java.util.Optional;
import java.util.function.Predicate;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RetryUtils {
    interface Task extends org.embulk.util.config.Task {
        @Config("max_connection_retry")
        @ConfigDefault("10") // 10 times retry to connect GCS server if failed.
        int getMaxConnectionRetry();

        @Config("initial_retry_interval_millis")
        @ConfigDefault("1000")
        int getInitialRetryIntervalMillis();

        @Config("maximum_retry_interval_millis")
        @ConfigDefault("300000")
        int getMaximumRetryIntervalMillis();
    }

    private RetryUtils() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(RetryUtils.class);

    /**
     * A utility predicate to detect status code 4xx of `GoogleJsonResponseException`
     */
    private static final Predicate<GoogleJsonResponseException> API_ERROR_NOT_RETRY_4XX = e -> {
        if (e.getDetails() == null && e.getContent() != null) {
            LOG.warn("Invalid response was returned : {}", e.getContent());
            return true;
        }
        int statusCode = e.getDetails().getCode();
        return statusCode / 100 != 4;
    };

    /**
     * A utility predicate to detect status code 4xx of `TokenResponseException`
     * But will retry 400 "Invalid JWS..."
     */
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

    /**
     * A default (abstract) retryable impl, which makes use of above 2 predicates
     * With default behaviors onRetry, etc.
     */
    public abstract static class DefaultRetryable<T> implements Retryable<T> {
        @Override
        public boolean isRetryableException(final Exception exception) {
            if (exception instanceof GoogleJsonResponseException) {
                return API_ERROR_NOT_RETRY_4XX.test((GoogleJsonResponseException) exception);
            } else if (exception instanceof TokenResponseException) {
                return TOKEN_ERROR_NOT_RETRY_4XX.test((TokenResponseException) exception);
            }
            return true;
        }

        @Override
        public void onRetry(final Exception exception, final int retryCount, final int retryLimit, final int retryWait) {
            String message = String.format("GCS GET request failed. Retrying %d/%d after %d seconds. Message: %s: %s",
                    retryCount, retryLimit, retryWait / 1000, exception.getClass(), exception.getMessage());
            if (retryCount % 3 == 0) {
                LOG.warn(message, exception);
            } else {
                LOG.warn(message);
            }
        }

        @Override
        public void onGiveup(final Exception firstException, final Exception lastException) {
        }
    }

    /**
     * Return Blob GET op that is ready for {@code withRetry}
     */
    static DefaultRetryable<Blob> get(final Storage client, final String bucket, final String key) {
        return new DefaultRetryable<Blob>() {
            @Override
            public Blob call() {
                return client.get(bucket, key);
            }
        };
    }

    /**
     * Utility method
     */
    static <T> T withRetry(final Task task, final Retryable<T> op) {
        try {
            return RetryExecutor.builder()
                    .withInitialRetryWaitMillis(task.getInitialRetryIntervalMillis())
                    .withMaxRetryWaitMillis(task.getMaximumRetryIntervalMillis())
                    .withRetryLimit(task.getMaxConnectionRetry())
                    .build()
                    .runInterruptible(op);
        } catch (final RetryGiveupException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
