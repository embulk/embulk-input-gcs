package org.embulk.input.gcs;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.embulk.config.ConfigException;
import org.embulk.spi.unit.LocalFile;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

class ServiceUtils
{
    private ServiceUtils()
    {}

    static Storage newClient(final Optional<LocalFile> jsonKeyfile)
    {
        if (jsonKeyfile.isPresent()) {
            try {
                final String path = jsonKeyfile.map(f -> f.getPath().toString()).get();
                final InputStream jsonStream = new FileInputStream(path);
                final Credentials credentials = ServiceAccountCredentials.fromStream(jsonStream);
                // TODO: test client to verify auth
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
