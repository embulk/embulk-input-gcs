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

import static org.embulk.input.gcs.GcsFileInputPlugin.CONFIG_MAPPER_FACTORY;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.util.file.InputStreamFileInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsFileInput extends InputStreamFileInput implements TransactionalFileInput {
    private static final Logger LOG = LoggerFactory.getLogger(org.embulk.input.gcs.GcsFileInput.class);

    GcsFileInput(final PluginTask task, final int taskIndex) {
        super(Exec.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
    }

    public void abort() {
    }

    public TaskReport commit() {
        return CONFIG_MAPPER_FACTORY.newTaskReport();
    }

    @Override
    public void close() {
    }

    /**
     * Lists GCS filenames filtered by prefix.
     *
     * The resulting list does not include the file that's size == 0.
     */
    static FileList listFiles(final PluginTask task) {
        Storage client = AuthUtils.newClient(task);
        String bucket = task.getBucket();

        // @see https://cloud.google.com/storage/docs/json_api/v1/buckets/get
        if (LOG.isDebugEnabled()) {
            printBucketInfo(client, bucket);
        }

        String prefix = task.getPathPrefix().orElse("");
        String lastKey = task.getLastPath().isPresent() ? base64Encode(task.getLastPath().get()) : "";
        FileList.Builder builder = new FileList.Builder(task);

        try {
            // @see https://cloud.google.com/storage/docs/json_api/v1/objects/list
            Page<Blob> blobs = client.list(bucket, Storage.BlobListOption.prefix(prefix), Storage.BlobListOption.pageToken(lastKey));
            for (Blob blob : blobs.iterateAll()) {
                if (blob.getSize() > 0) {
                    builder.add(blob.getName(), blob.getSize());
                }
                LOG.debug("filename: {}", blob.getName());
                LOG.debug("updated: {}", blob.getUpdateTime());
            }
        } catch (final RuntimeException e) {
            if ((e instanceof StorageException) && ((StorageException) e).getCode() == 400) {
                throw new ConfigException(String.format("Files listing failed: bucket:%s, prefix:%s, last_path:%s", bucket, prefix, lastKey), e);
            }

            LOG.warn(String.format("Could not get file list from bucket:%s", bucket));
            LOG.warn(e.getMessage());
        }
        return builder.build();
    }

    // String nextToken = base64Encode(0x0a + ASCII character according to utf8EncodeLength position+ filePath);
    static String base64Encode(final String path) {
        byte[] lengthVarint;
        byte[] encoding;
        byte[] utf8 = path.getBytes(StandardCharsets.UTF_8);
        LOG.debug("path string: {} ,path length:{} \" + ", path, utf8.length);

        int utf8EncodeLength = utf8.length;
        // GCP object names can be up to 1024 bytes in length.
        // This limit aligns with task.getLastPath() expectations.
        if (utf8EncodeLength >= 1025) {
            throw new ConfigException(String.format("last_path '%s' is too long to encode. Maximum allowed is 1024 bytes", path));
        }

        lengthVarint = encodeVarint(utf8EncodeLength);
        encoding = new byte[1 + lengthVarint.length + utf8.length];
        encoding[0] = 0x0a;

        System.arraycopy(lengthVarint, 0, encoding, 1, lengthVarint.length);
        System.arraycopy(utf8, 0, encoding, 1 + lengthVarint.length, utf8.length);

        final String s = Base64.getEncoder().encodeToString(encoding);
        LOG.debug("last_path(base64 encoded): {}", s);
        return s;
    }

    // see: https://protobuf.dev/programming-guides/encoding/#varints
    private static byte[] encodeVarint(int value)
    {
        // utf8EncodeLength.length is up to 65535, so 2 bytes are enough for buffer
        byte[] buffer = new byte[2];
        int pos = 0;
        while (true) {
            int bits = value & 0x7F;
            value >>>= 7;
            if (value != 0) {
                buffer[pos++] = (byte) (bits | 0x80);
            }
            else {
                buffer[pos++] = (byte) bits;
                break;
            }
        }
        byte[] result = new byte[pos];
        System.arraycopy(buffer, 0, result, 0, pos);
        return result;
    }

    private static void printBucketInfo(final Storage client, final String bucket) {
        // get Bucket
        Storage.BucketGetOption fields = Storage.BucketGetOption.fields(
                Storage.BucketField.LOCATION,
                Storage.BucketField.TIME_CREATED,
                Storage.BucketField.OWNER
        );
        com.google.cloud.storage.Bucket bk = client.get(bucket, fields);
        LOG.debug("bucket name: {}", bk.getName());
        LOG.debug("bucket location: {}", bk.getLocation());
        LOG.debug("bucket timeCreated: {}", bk.getCreateTime());
        LOG.debug("bucket owner: {}", bk.getOwner());
    }
}
