/*
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

#ifndef CODED_STREAM_WRAPPER_HH
#define CODED_STREAM_WRAPPER_HH

#include "Adaptor.hh"

DIAGNOSTIC_PUSH

#ifdef __clang__
  DIAGNOSTIC_IGNORE("-Wshorten-64-to-32")
  DIAGNOSTIC_IGNORE("-Wreserved-id-macro")
#endif

#if defined(__GNUC__) || defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wconversion")
#endif

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>

DIAGNOSTIC_POP

namespace orc {
  // ORC-2187 (apache/orc#2661): match the Java reader's 1 GiB message limit.
  constexpr int PROTOBUF_MESSAGE_MAX_LIMIT = 1024 << 20;

  template <typename Message>
  inline bool parseProtobufFromStream(
      Message* message, google::protobuf::io::ZeroCopyInputStream* input) {
    {
      google::protobuf::io::CodedInputStream codedStream(input);
      codedStream.SetTotalBytesLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
      // Coded-stream parsing alone also accepts an end-group tag or a zero tag.
      if (!message->ParseFromCodedStream(&codedStream) ||
          !codedStream.ConsumedEntireMessage()) {
        return false;
      }
    }
    // Old protobuf can treat the byte limit as EOF. Its destructor first
    // restores unread buffers; only underlying EOF proves a complete message.
    const void* data;
    int size;
    while (input->Next(&data, &size)) {
      // Zero-sized chunks are legal and do not indicate EOF.
      if (size != 0) {
        input->BackUp(size);
        return false;
      }
    }
    return true;
  }
}

#endif
