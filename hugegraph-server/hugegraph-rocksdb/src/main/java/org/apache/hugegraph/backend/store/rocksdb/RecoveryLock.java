/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.backend.store.rocksdb;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;

/** Owns the descriptor and its physical-file reservation through actual close completion. */
final class RecoveryLock implements AutoCloseable {

    private static final Set<RecoveryLock> OWNERS = new HashSet<>();

    private final FileChannel channel;
    private final Object identity;
    private final Path path;
    private boolean closed;
    private Throwable closeFailure;

    private RecoveryLock(FileChannel channel, Object identity, Path path) {
        this.channel = channel;
        this.identity = identity;
        this.path = path;
    }

    static RecoveryLock acquire(Path path, String data) throws IOException {
        RecoveryLock owner;
        synchronized (OWNERS) {
            // CREATE_NEW never opens an already owned inode. Opening then closing a
            // second descriptor can drop this process's existing POSIX record lock.
            try {
                Files.createFile(path);
            } catch (FileAlreadyExistsException ignored) {
                // Read its identity without opening another descriptor.
            }
            BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
            if (!attributes.isRegularFile()) {
                throw new IOException("Recovery lock must be a regular file: " + path);
            }
            Object identity = attributes.fileKey();
            // Prefer physical keys; the JDK permits providers with no fileKey.
            // isSameFile also recognizes aliases and propagates an identity lookup failure.
            for (RecoveryLock active : OWNERS) {
                boolean same = identity != null && active.identity != null ? identity.equals(active.identity) :
                               Files.isSameFile(path, active.path);
                if (same) {
                    throw new OverlappingFileLockException();
                }
            }
            owner = new RecoveryLock(FileChannel.open(path, StandardOpenOption.WRITE), identity, path);
            OWNERS.add(owner);
        }
        try {
            if (owner.channel.tryLock() == null) {
                throw new IOException("Database is already open or recovering: " + data);
            }
            return owner;
        } catch (IOException | RuntimeException | Error e) {
            try {
                owner.close();
            } catch (IOException | RuntimeException | Error closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    synchronized boolean isOpen() {
        return !this.closed && this.closeFailure == null;
    }

    @Override
    public synchronized void close() throws IOException {
        if (this.closed) {
            return;
        }
        if (this.closeFailure instanceof IOException) {
            throw (IOException) this.closeFailure;
        }
        if (this.closeFailure instanceof RuntimeException) {
            throw (RuntimeException) this.closeFailure;
        }
        if (this.closeFailure instanceof Error) {
            throw (Error) this.closeFailure;
        }
        try {
            // AbstractInterruptibleChannel flips isOpen before implCloseChannel returns.
            // Reserve this file until close finishes, without blocking unrelated owners.
            this.channel.close();
        } catch (IOException | RuntimeException | Error e) {
            this.closeFailure = e;
            throw e;
        }
        this.closed = true;
        synchronized (OWNERS) {
            OWNERS.remove(this);
        }
    }
}
