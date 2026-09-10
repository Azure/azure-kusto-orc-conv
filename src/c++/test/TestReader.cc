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
#include "orc/Exceptions.hh"

#include "Adaptor.hh"
#include "MemoryInputStream.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"
#include "wrap/orc-proto-wrapper.hh"

#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>

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

  class ReportedLengthInputStream : public InputStream {
  public:
    ReportedLengthInputStream(const std::string& buffer, uint64_t offset,
                              uint64_t length) :
      buffer(buffer), offset(offset), length(length), name("ReportedLengthInputStream") {
    }

    uint64_t getLength() const override {
      return length;
    }

    uint64_t getNaturalReadSize() const override {
      return 1024;
    }

    void read(void* output, uint64_t readLength, uint64_t readOffset) override {
      if (readOffset != offset || readLength > buffer.length()) {
        throw std::logic_error("Unexpected read from ReportedLengthInputStream");
      }
      memcpy(output, buffer.data(), static_cast<size_t>(readLength));
    }

    const std::string& getName() const override {
      return name;
    }

  private:
    const std::string& buffer;
    uint64_t offset;
    uint64_t length;
    std::string name;
  };

  proto::Footer makeFooter() {
    proto::Footer footer;
    footer.add_types()->set_kind(proto::Type_Kind_STRUCT);
    footer.set_numberofrows(0);
    return footer;
  }

  proto::PostScript makePostScript(uint64_t footerLength, uint64_t metadataLength) {
    proto::PostScript postscript;
    postscript.set_footerlength(footerLength);
    postscript.set_metadatalength(metadataLength);
    postscript.set_compression(proto::NONE);
    postscript.set_magic("ORC");
    return postscript;
  }

  std::string makeFile(const proto::Footer& footer, const proto::PostScript& postscript) {
    std::string serializedFooter;
    std::string serializedPostscript;
    EXPECT_TRUE(footer.SerializeToString(&serializedFooter));
    EXPECT_TRUE(postscript.SerializeToString(&serializedPostscript));
    EXPECT_LT(serializedPostscript.length(), 256U);
    return "ORC" + serializedFooter + serializedPostscript +
      static_cast<char>(serializedPostscript.length());
  }

  std::string makeFileTail(const proto::Footer& footer,
                           const proto::PostScript& postscript,
                           uint64_t fileLength) {
    proto::FileTail tail;
    tail.mutable_footer()->CopyFrom(footer);
    tail.mutable_postscript()->CopyFrom(postscript);
    tail.set_filelength(fileLength);
    tail.set_postscriptlength(0);
    std::string serializedTail;
    EXPECT_TRUE(tail.SerializeToString(&serializedTail));
    return serializedTail;
  }

  void expectParseError(const std::string& expectedMessage,
                        const std::function<void()>& action) {
    try {
      action();
      FAIL() << "Expected ParseError";
    } catch (const ParseError& error) {
      EXPECT_NE(std::string(error.what()).find(expectedMessage), std::string::npos);
    }
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

  TEST(TestReader, footerLengthOverflow) {
    proto::PostScript postscript = makePostScript(
      (std::numeric_limits<uint64_t>::max)(), 0);
    std::string file = makeFile(makeFooter(), postscript);

    expectParseError("Invalid ORC tailSize=", [&file]() {
      ReaderOptions options;
      createReader(std::unique_ptr<InputStream>(
        new MemoryInputStream(file.data(), file.length())), options);
    });
  }

  TEST(TestReader, metadataLengthOverflow) {
    proto::Footer footer = makeFooter();
    proto::PostScript postscript = makePostScript(
      1, (std::numeric_limits<uint64_t>::max)());
    std::string tail = makeFileTail(footer, postscript, 100);
    const std::string file;

    ReaderOptions options;
    options.setSerializedFileTail(tail);
    std::unique_ptr<Reader> reader = createReader(std::unique_ptr<InputStream>(
      new MemoryInputStream(file.data(), file.length())), options);
    expectParseError("Invalid Metadata length:", [&reader]() {
      reader->getNumberOfStripeStatistics();
    });
  }

  TEST(TestReader, stripeLengthOverflow) {
    proto::Footer footer = makeFooter();
    proto::StripeInformation* stripe = footer.add_stripes();
    stripe->set_offset((std::numeric_limits<uint64_t>::max)() - 1);
    stripe->set_indexlength(2);
    stripe->set_numberofrows(1);
    proto::PostScript postscript = makePostScript(1, 0);
    std::string tail = makeFileTail(footer, postscript,
                                    (std::numeric_limits<uint64_t>::max)());
    const std::string file;

    ReaderOptions options;
    options.setSerializedFileTail(tail);
    std::unique_ptr<Reader> reader = createReader(std::unique_ptr<InputStream>(
      new ReportedLengthInputStream(file, stripe->offset(),
                                    (std::numeric_limits<uint64_t>::max)())), options);
    std::unique_ptr<RowReader> rowReader = reader->createRowReader();
    std::unique_ptr<ColumnVectorBatch> batch = rowReader->createRowBatch(1);
    expectParseError("Malformed StripeInformation", [&rowReader, &batch]() {
      rowReader->next(*batch);
    });
  }

  TEST(TestReader, largeStripeLength) {
    proto::Footer footer = makeFooter();
    proto::StripeInformation* stripe = footer.add_stripes();
    proto::StripeFooter stripeFooter;
    stripeFooter.add_columns();
    std::string serializedStripeFooter;
    EXPECT_TRUE(stripeFooter.SerializeToString(&serializedStripeFooter));
    uint64_t fileLength = (std::numeric_limits<uint64_t>::max)();
    stripe->set_offset(fileLength - serializedStripeFooter.length() - 1);
    stripe->set_footerlength(serializedStripeFooter.length());
    stripe->set_numberofrows(1);
    proto::PostScript postscript = makePostScript(1, 0);
    std::string tail = makeFileTail(footer, postscript, fileLength);

    ReaderOptions options;
    options.setSerializedFileTail(tail);
    std::unique_ptr<Reader> reader = createReader(std::unique_ptr<InputStream>(
      new ReportedLengthInputStream(serializedStripeFooter,
                                    stripe->offset(), fileLength)), options);
    std::unique_ptr<RowReader> rowReader = reader->createRowReader();
    std::unique_ptr<ColumnVectorBatch> batch = rowReader->createRowBatch(1);
    EXPECT_TRUE(rowReader->next(*batch));
  }

}  // namespace
