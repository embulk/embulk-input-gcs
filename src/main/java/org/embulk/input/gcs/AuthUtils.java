package org.embulk.input.gcs;

import com.google.api.client.util.SecurityUtils;
import com.google.api.services.storage.StorageScopes;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import org.embulk.config.ConfigException;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.units.LocalFile;

class AuthUtils {
    public enum AuthMethod {
        private_key, compute_engine, json_key
    }

    interface Task {
        @Config("auth_method")
        @ConfigDefault("\"private_key\"")
        AuthUtils.AuthMethod getAuthMethod();

        @Config("service_account_email")
        @ConfigDefault("null")
        Optional<String> getServiceAccountEmail();

        // kept for backward compatibility
        @Config("p12_keyfile_fullpath")
        @ConfigDefault("null")
        Optional<String> getP12KeyfileFullpath();

        @Config("p12_keyfile")
        @ConfigDefault("null")
        Optional<LocalFile> getP12Keyfile();

        void setP12Keyfile(Optional<LocalFile> p12Keyfile);

        @Config("json_keyfile")
        @ConfigDefault("null")
        Optional<LocalFile> getJsonKeyfile();
    }

    private AuthUtils() {
    }

    static Storage newClient(final PluginTask task) {
        try {
            final StorageOptions.Builder builder = StorageOptions.newBuilder();
            switch (task.getAuthMethod()) {
                case json_key:
                    builder.setCredentials(fromJson(task));
                    break;
                case private_key:
                    builder.setCredentials(fromP12(task));
                    break;
                default:
                    // compute_engine does not need credentials
                    break;
            }
            // test client to verify auth
            final Storage client = builder.build().getService();
            client.list(task.getBucket(), Storage.BlobListOption.pageSize(1));
            return client;
        } catch (final StorageException | IOException | GeneralSecurityException e) {
            throw new ConfigException(e);
        }
    }

    static Credentials fromP12(final Task task) throws IOException, GeneralSecurityException {
        final String path = task.getP12Keyfile().get().getPath().toString();
        try (final InputStream p12InputStream = new FileInputStream(path)) {
            final PrivateKey pk = SecurityUtils.loadPrivateKeyFromKeyStore(
                    SecurityUtils.getPkcs12KeyStore(), p12InputStream, "notasecret",
                    "privatekey", "notasecret");
            final ArrayList<String> scopes = new ArrayList<>();
            scopes.add(StorageScopes.DEVSTORAGE_READ_ONLY);
            return ServiceAccountCredentials.newBuilder()
                    .setClientEmail(task.getServiceAccountEmail().orElse(null))
                    .setPrivateKey(pk)
                    .setScopes(Collections.unmodifiableList(scopes))
                    .build();
        }
    }

    static Credentials fromJson(final Task task) throws IOException {
        final String path = task.getJsonKeyfile().map(f -> f.getPath().toString()).get();
        final InputStream jsonStream = new FileInputStream(path);
        return ServiceAccountCredentials.fromStream(jsonStream);
    }
}
