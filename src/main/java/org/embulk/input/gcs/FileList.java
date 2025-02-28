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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;

public class FileList {
    public interface Task {
        @Config("path_match_pattern")
        @ConfigDefault("\".*\"")
        String getPathMatchPattern();

        @Config("total_file_count_limit")
        @ConfigDefault("2147483647")
        int getTotalFileCountLimit();

        // TODO support more algorithms to combine tasks
        @Config("min_task_size")
        @ConfigDefault("0")
        long getMinTaskSize();
    }

    public static class Entry {
        private int index;
        private long size;

        @JsonCreator
        public Entry(
                @JsonProperty("index") final int index,
                @JsonProperty("size") final long size) {
            this.index = index;
            this.size = size;
        }

        @JsonProperty("index")
        public int getIndex() {
            return index;
        }

        @JsonProperty("size")
        public long getSize() {
            return size;
        }
    }

    public static class Builder {
        private final ByteArrayOutputStream binary;
        private final OutputStream stream;
        private final List<Entry> entries = new ArrayList<>();
        private String last = null;

        private int limitCount = Integer.MAX_VALUE;
        private long minTaskSize = 1;
        private Pattern pathMatchPattern;

        private final ByteBuffer castBuffer = ByteBuffer.allocate(4);

        public Builder(final Task task) {
            this();
            this.pathMatchPattern = Pattern.compile(task.getPathMatchPattern());
            this.limitCount = task.getTotalFileCountLimit();
            this.minTaskSize = task.getMinTaskSize();
        }

        public Builder(final ConfigSource config) {
            this();
            this.pathMatchPattern = Pattern.compile(config.get(String.class, "path_match_pattern", ".*"));
            this.limitCount = config.get(int.class, "total_file_count_limit", Integer.MAX_VALUE);
            this.minTaskSize = config.get(long.class, "min_task_size", 0L);
        }

        public Builder() {
            binary = new ByteArrayOutputStream();
            try {
                stream = new BufferedOutputStream(new GZIPOutputStream(binary));
            } catch (final IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        public Builder limitTotalFileCount(final int limitCount) {
            this.limitCount = limitCount;
            return this;
        }

        public Builder minTaskSize(final long bytes) {
            this.minTaskSize = bytes;
            return this;
        }

        public synchronized Builder pathMatchPattern(final String pattern) {
            this.pathMatchPattern = Pattern.compile(pattern);
            return this;
        }

        public int size() {
            return entries.size();
        }

        public boolean needsMore() {
            return size() < limitCount;
        }

        // returns true if this file is used
        public synchronized boolean add(final String path, final long size) {
            // TODO throw IllegalStateException if stream is already closed

            if (!needsMore()) {
                return false;
            }

            if (!pathMatchPattern.matcher(path).find()) {
                return false;
            }

            int index = entries.size();
            entries.add(new Entry(index, size));

            byte[] data = path.getBytes(StandardCharsets.UTF_8);
            castBuffer.putInt(0, data.length);
            try {
                stream.write(castBuffer.array());
                stream.write(data);
            } catch (final IOException ex) {
                throw new RuntimeException(ex);
            }

            last = path;
            return true;
        }

        public synchronized FileList build() {
            try {
                stream.close();
            } catch (final IOException ex) {
                throw new RuntimeException(ex);
            }
            return new FileList(binary.toByteArray(), getSplits(entries), Optional.ofNullable(last));
        }

        private List<List<Entry>> getSplits(final List<Entry> all) {
            List<List<Entry>> tasks = new ArrayList<>();
            long currentTaskSize = 0;
            List<Entry> currentTask = new ArrayList<>();
            for (Entry entry : all) {
                currentTask.add(entry);
                currentTaskSize += entry.getSize();  // TODO consider to multiply the size by cost_per_byte, and add cost_per_file
                if (currentTaskSize >= minTaskSize) {
                    tasks.add(currentTask);
                    currentTask = new ArrayList<>();
                    currentTaskSize = 0;
                }
            }
            if (!currentTask.isEmpty()) {
                tasks.add(currentTask);
            }
            return tasks;
        }
    }

    private final byte[] data;
    private final List<List<Entry>> tasks;
    private final Optional<String> last;

    @JsonCreator
    @Deprecated
    public FileList(
            @JsonProperty("data") final byte[] data,
            @JsonProperty("tasks") final List<List<Entry>> tasks,
            @JsonProperty("last") final Optional<String> last) {
        this.data = data.clone();
        this.tasks = tasks;
        this.last = last;
    }

    @JsonIgnore
    public Optional<String> getLastPath(final Optional<String> lastLastPath) {
        if (last.isPresent()) {
            return last;
        }
        return lastLastPath;
    }

    @JsonIgnore
    public int getTaskCount() {
        return tasks.size();
    }

    @JsonIgnore
    public List<String> get(final int i) {
        return new EntryList(data, tasks.get(i));
    }

    @JsonProperty("data")
    @Deprecated
    public byte[] getData() {
        return data.clone();
    }

    @JsonProperty("tasks")
    @Deprecated
    public List<List<Entry>> getTasks() {
        return tasks;
    }

    @JsonProperty("last")
    @Deprecated
    public Optional<String> getLast() {
        return last;
    }

    private static class EntryList extends AbstractList<String> {
        private final byte[] data;
        private final List<Entry> entries;
        private InputStream stream;
        private int current;

        private final ByteBuffer castBuffer = ByteBuffer.allocate(4);

        public EntryList(final byte[] data, final List<Entry> entries) {
            this.data = data;
            this.entries = entries;
            try {
                this.stream = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(data)));
            } catch (final IOException ex) {
                throw new RuntimeException(ex);
            }
            this.current = 0;
        }

        @Override
        public synchronized String get(final int i) {
            Entry e = entries.get(i);
            if (e.getIndex() < current) {
                // rewind to the head
                try {
                    stream.close();
                    stream = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(data)));
                } catch (final IOException ex) {
                    throw new RuntimeException(ex);
                }
                current = 0;
            }

            while (current < e.getIndex()) {
                readNext();
            }
            // now current == e.getIndex()
            return readNextString();
        }

        @Override
        public int size() {
            return entries.size();
        }

        private byte[] readNext() {
            try {
                int n = stream.read(castBuffer.array());
                if (n != castBuffer.capacity()) {
                    throw new IllegalArgumentException(
                            "Unexpected stream close, expecting " + castBuffer.capacity() + " bytes, but received " + n + " bytes");
                }
                int len = castBuffer.getInt(0);
                byte[] b = new byte[len];  // here should be able to use a pooled buffer because read data is ignored if readNextString doesn't call this method
                n = stream.read(b);
                if (n != len) {
                    throw new IllegalArgumentException(
                            "Unexpected stream close, expecting " + castBuffer.capacity() + " bytes, but received " + n + " bytes");
                }

                current++;

                return b;
            } catch (final IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        private String readNextString() {
            return new String(readNext(), StandardCharsets.UTF_8);
        }
    }
}
