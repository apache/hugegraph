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

package org.apache.hugegraph.rocksdb.access.cloud;

import java.net.URI;

import org.apache.hugegraph.config.HugeConfig;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

/**
 * S3CompatibleStorageProvider provides support for S3-compatible cloud storage.
 * Supports:
 * - AWS S3
 * - MinIO
 * - LocalStack
 * - DigitalOcean Spaces
 * - Wasabi
 * - Any other S3-compatible object storage service
 * This is a built-in provider included in the core hg-store-rocksdb module.
 */
public class S3CompatibleStorageProvider implements CloudStorageProvider {

    @Override
    public String name() {
        return "s3";
    }

    @Override
    public CloudStorageClient create(HugeConfig config) throws Exception {
        S3Client s3Client = buildS3Client(config);
        return new S3CompatibleStorageClient(s3Client);
    }

    /**
     * Build an S3Client from HugeConfig.
     *
     * @param config HugeConfig containing S3 configuration
     * @return configured S3Client
     */
    private static S3Client buildS3Client(HugeConfig config) {
        String region = getString(config, "us-east-1", "rocksdb.cloud_region");
        String endpoint = getString(config, "", "rocksdb.cloud_endpoint");
        String accessKey = getString(config, "", "rocksdb.cloud_access_key");
        String secretKey = getString(config, "", "rocksdb.cloud_secret_key");
        boolean pathStyle = getBoolean(config);

        S3ClientBuilder builder = S3Client.builder();

        // Set region (used for AWS S3; some S3-compatible services may ignore this)
        builder.region(Region.of(region));

        // Configure credentials
        AwsCredentialsProvider credentialsProvider;
        if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            // Use provided credentials
            credentialsProvider = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey));
        } else {
            // Use default credential provider chain (IAM, environment variables, etc.)
            credentialsProvider = DefaultCredentialsProvider.create();
        }
        builder.credentialsProvider(credentialsProvider);

        // Configure endpoint for S3-compatible services (MinIO, LocalStack, etc.)
        if (!endpoint.isEmpty()) {
            builder.endpointOverride(URI.create(endpoint));

            // Enable path-style addressing for S3-compatible services
            S3Configuration s3Config = S3Configuration.builder()
                    .pathStyleAccessEnabled(pathStyle)
                    .build();
            builder.serviceConfiguration(s3Config);
        }

        return builder.build();
    }

    /**
     * Get a string configuration value from the provided candidate keys.
     */
    private static String getString(HugeConfig config, String defaultValue, String... keys) {
        String value = null;
        for (String key : keys) {
            if (config.containsKey(key)) {
                value = String.valueOf(config.getProperty(key));
                break;
            }
        }
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        return value.trim();
    }

    /**
     * Get a boolean configuration value from the provided candidate keys.
     */
    private static boolean getBoolean(HugeConfig config) {
        return Boolean.parseBoolean(getString(config, String.valueOf(false),
                                              "rocksdb.cloud_path_style"));
    }
}

