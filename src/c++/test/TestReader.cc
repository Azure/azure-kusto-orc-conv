/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Reader.hh"

#include "Adaptor.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

#include <limits>
#include <stdexcept>

namespace orc {

  uint64_t getCompressionBlockSize(const proto::PostScript& ps);

  proto::PostScript serializedPostScript(uint64_t compressionBlockSize,
                                         bool setCompressionBlockSize) {
    proto::PostScript original;
    original.set_footerlength(0);
    original.set_compression(proto::NONE);
    original.set_metadatalength(0);
    original.add_version(0);
    original.add_version(12);
    original.set_magic("ORC");
    if (setCompressionBlockSize) {
      original.set_compressionblocksize(compressionBlockSize);
    }

    std::string serialized;
    if (!original.SerializeToString(&serialized)) {
      throw std::runtime_error("Failed to serialize PostScript");
    }

    proto::PostScript parsed;
    if (!parsed.ParseFromString(serialized)) {
      throw std::runtime_error("Failed to parse PostScript");
    }
    return parsed;
  }

  TEST(TestReader, testCompressionBlockSize) {
    EXPECT_EQ(256 * 1024,
              getCompressionBlockSize(serializedPostScript(0, false)));
    EXPECT_EQ(1, getCompressionBlockSize(serializedPostScript(1, true)));
    EXPECT_EQ((1 << 23) - 1,
              getCompressionBlockSize(serializedPostScript((1 << 23) - 1,
                                                            true)));

    EXPECT_THROW(getCompressionBlockSize(serializedPostScript(0, true)),
                 ParseError);
    EXPECT_THROW(getCompressionBlockSize(serializedPostScript(1 << 23, true)),
                 ParseError);
    EXPECT_THROW(getCompressionBlockSize(
                     serializedPostScript((std::numeric_limits<uint64_t>::max)(),
                                          true)),
                 ParseError);
  }

  TEST(TestReader, testWriterVersions) {
    EXPECT_EQ("original", writerVersionToString(WriterVersion_ORIGINAL));
    EXPECT_EQ("HIVE-8732", writerVersionToString(WriterVersion_HIVE_8732));
    EXPECT_EQ("HIVE-4243", writerVersionToString(WriterVersion_HIVE_4243));
    EXPECT_EQ("HIVE-12055", writerVersionToString(WriterVersion_HIVE_12055));
    EXPECT_EQ("HIVE-13083", writerVersionToString(WriterVersion_HIVE_13083));
    EXPECT_EQ("future - 99",
              writerVersionToString(static_cast<WriterVersion>(99)));
  }

  TEST(TestReader, testCompressionNames) {
    EXPECT_EQ("none", compressionKindToString(CompressionKind_NONE));
    EXPECT_EQ("zlib", compressionKindToString(CompressionKind_ZLIB));
    EXPECT_EQ("snappy", compressionKindToString(CompressionKind_SNAPPY));
    EXPECT_EQ("lzo", compressionKindToString(CompressionKind_LZO));
    EXPECT_EQ("lz4", compressionKindToString(CompressionKind_LZ4));
    EXPECT_EQ("zstd", compressionKindToString(CompressionKind_ZSTD));
    EXPECT_EQ("unknown - 99",
              compressionKindToString(static_cast<CompressionKind>(99)));
  }

}  // namespace
