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

#include "orc/ColumnPrinter.hh"
#include "orc/orc-config.hh"

#include "Adaptor.hh"

#include <limits>
#include <sstream>
#include <stdexcept>
#include <time.h>
#include <typeinfo>
#include <ctype.h>

#ifdef __clang__
  #pragma clang diagnostic ignored "-Wformat-security"
#endif

namespace orc {

  class VoidColumnPrinter: public ColumnPrinter {
  public:
    VoidColumnPrinter(std::string&);
    ~VoidColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class BooleanColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;
  public:
    BooleanColumnPrinter(std::string&);
    ~BooleanColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class LongColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;
  public:
    LongColumnPrinter(std::string&);
    ~LongColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class DoubleColumnPrinter: public ColumnPrinter {
  private:
    const double* data;
    const bool isFloat;

  public:
    DoubleColumnPrinter(std::string&, const Type& type);
    virtual ~DoubleColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class TimestampColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* seconds;
    const int64_t* nanoseconds;

  public:
    TimestampColumnPrinter(std::string&);
    ~TimestampColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class DateColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;

  public:
    DateColumnPrinter(std::string&);
    ~DateColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class Decimal64ColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;
    int32_t scale;
  public:
    Decimal64ColumnPrinter(std::string&);
    ~Decimal64ColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class Decimal128ColumnPrinter: public ColumnPrinter {
  private:
    const Int128* data;
    int32_t scale;
  public:
    Decimal128ColumnPrinter(std::string&);
    ~Decimal128ColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class StringColumnPrinter: public ColumnPrinter {
  private:
    const char* const * start;
    const int64_t* length;
  public:
    StringColumnPrinter(std::string&);
    virtual ~StringColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class BinaryColumnPrinter: public ColumnPrinter {
  private:
    const char* const * start;
    const int64_t* length;
  public:
    BinaryColumnPrinter(std::string&);
    virtual ~BinaryColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class ListColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* offsets;
    std::unique_ptr<ColumnPrinter> elementPrinter;

  public:
    ListColumnPrinter(std::string&, const Type& type);
    virtual ~ListColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class MapColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* offsets;
    std::unique_ptr<ColumnPrinter> keyPrinter;
    std::unique_ptr<ColumnPrinter> elementPrinter;

  public:
    MapColumnPrinter(std::string&, const Type& type);
    virtual ~MapColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class UnionColumnPrinter: public ColumnPrinter {
  private:
    const unsigned char *tags;
    const uint64_t* offsets;
    std::vector<std::unique_ptr<ColumnPrinter>> fieldPrinter;

  public:
    UnionColumnPrinter(std::string&, const Type& type);
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class StructColumnPrinter: public ColumnPrinter {
  private:
    std::vector<std::unique_ptr<ColumnPrinter>> fieldPrinter;
    std::vector<std::string> fieldNames;
  public:
    StructColumnPrinter(std::string&, const Type& type);
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  inline void writeChar(std::string& file, char ch) {
    file += ch;
  }

  inline void writeString(std::string& file, const char *ptr, int64_t len) {
    file.append(ptr, len);
  }

  inline void writeNull(std::string& file) {
    file.append("null", sizeof("null")-1);
  }

  static const uint8_t UTF8_T[] = {
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, // 00..1f
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, // 20..3f
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, // 40..5f
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, // 60..7f
    1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9, // 80..9f
    7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7, // a0..bf
    8,8,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2, // c0..df
    0xa,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x4,0x3,0x3, // e0..ef
    0xb,0x6,0x6,0x6,0x5,0x8,0x8,0x8,0x8,0x8,0x8,0x8,0x8,0x8,0x8,0x8, // f0..ff
    0x0,0x1,0x2,0x3,0x5,0x8,0x7,0x1,0x1,0x1,0x4,0x6,0x1,0x1,0x1,0x1, // s0..s0
    1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,0,1,0,1,1,1,1,1,1, // s1..s2
    1,2,1,1,1,1,1,2,1,2,1,1,1,1,1,1,1,1,1,1,1,1,1,2,1,1,1,1,1,1,1,1, // s3..s4
    1,2,1,1,1,1,1,1,1,2,1,1,1,1,1,1,1,1,1,1,1,1,1,3,1,3,1,1,1,1,1,1, // s5..s6
    1,3,1,1,1,1,1,3,1,3,1,1,1,1,1,1,1,3,1,1,1,1,1,1,1,1,1,1,1,1,1,1, // s7..s8
  };

  bool isUtf8String(const char *ptr, size_t len) {
    const uint32_t state_utf8 = 0;
    const uint32_t state_not_utf8 = 1;

    uint32_t state = state_utf8;
    for (size_t i = 0; i < len; i++) {
        uint8_t type = UTF8_T[(uint8_t) ptr[i]];
        state = UTF8_T[256 + state * 16 + type];
        if (state == state_not_utf8) {
            break;
        }
    }
    return state == state_utf8;
  }

  const char *JSON_HEX_CHARS = "0123456789abcdefABCDEF";

  inline void writeQuotedChar(std::string& buffer, char ch) {
    // The following characters must be escaped according to JSON specification.
    switch (ch) {
    case '\\':
      writeString(buffer, "\\\\", 2);
      break;
    case '\b':
      writeString(buffer, "\\b", 2);
      break;
    case '\f':
      writeString(buffer, "\\f", 2);
      break;
    case '\n':
      writeString(buffer, "\\n", 2);
      break;
    case '\r':
      writeString(buffer, "\\r", 2);
      break;
    case '\t':
      writeString(buffer, "\\t", 2);
      break;
    case '"':
      writeString(buffer, "\\\"", 2);
      break;
    default:
      if (iscntrl(ch)) {
        writeString(buffer, "\\u00", sizeof("\\u00")-1);
        writeChar(buffer, JSON_HEX_CHARS[ch >> 4]);
        writeChar(buffer, JSON_HEX_CHARS[ch & 0xf]);
      } else {
        writeChar(buffer, ch);
      }
      break;
    }
  }

  // Writes characters as valid UTF-8 JSON string escaped with double-quotes.
  void writeQuotedString(std::string& buffer, const char *ptr, int64_t len) {
      writeChar(buffer, '"');

      if (!isUtf8String(ptr, len)) {
        for(int64_t i = 0; i < len; ++i) {
          char ch = ptr[i];
          writeQuotedChar(buffer, ch);
        }
      }
      else {
        for(int64_t i = 0; i < len; ++i) {
          char ch = ptr[i];
          if ((ch & 0xe0) == 0xc0) { // 2-byte UTF-8
            writeString(buffer, ptr + i, 2);
            i += 1;
          } else if ((ch & 0xf0) == 0xe0) { // 3-byte UTF-8
            writeString(buffer, ptr + i, 3);
            i += 2;
          } else if ((ch & 0xf8) == 0xf0) { // 4-byte UTF-8
            writeString(buffer, ptr + i, 4);
            i += 3;
          } else {
            writeQuotedChar(buffer, ch);
          }
        }
      }
      
      writeChar(buffer, '"');
  }

  ColumnPrinter::ColumnPrinter(std::string& _buffer
                               ): buffer(_buffer) {
    notNull = nullptr;
    hasNulls = false;
  }

  ColumnPrinter::~ColumnPrinter() {
    // PASS
  }

  void ColumnPrinter::reset(const ColumnVectorBatch& batch) {
    hasNulls = batch.hasNulls;
    if (hasNulls) {
      notNull = batch.notNull.data();
    } else {
      notNull = nullptr ;
    }
  }

  std::unique_ptr<ColumnPrinter> createColumnPrinter(std::string& buffer,
                                                     const Type* type) {
    ColumnPrinter *result = nullptr;
    if (type == nullptr) {
      result = new VoidColumnPrinter(buffer);
    } else {
      switch(static_cast<int64_t>(type->getKind())) {
      case BOOLEAN:
        result = new BooleanColumnPrinter(buffer);
        break;

      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        result = new LongColumnPrinter(buffer);
        break;

      case FLOAT:
      case DOUBLE:
        result = new DoubleColumnPrinter(buffer, *type);
        break;

      case STRING:
      case VARCHAR :
      case CHAR:
        result = new StringColumnPrinter(buffer);
        break;

      case BINARY:
        result = new BinaryColumnPrinter(buffer);
        break;

      case TIMESTAMP:
        result = new TimestampColumnPrinter(buffer);
        break;

      case LIST:
        result = new ListColumnPrinter(buffer, *type);
        break;

      case MAP:
        result = new MapColumnPrinter(buffer, *type);
        break;

      case STRUCT:
        result = new StructColumnPrinter(buffer, *type);
        break;

      case DECIMAL:
        if (type->getPrecision() == 0 || type->getPrecision() > 18) {
          result = new Decimal128ColumnPrinter(buffer);
        } else {
          result = new Decimal64ColumnPrinter(buffer);
        }
        break;

      case DATE:
        result = new DateColumnPrinter(buffer);
        break;

      case UNION:
        result = new UnionColumnPrinter(buffer, *type);
        break;

      default:
        throw std::logic_error("unknown batch type");
      }
    }
    return std::unique_ptr<ColumnPrinter>(result);
  }

  VoidColumnPrinter::VoidColumnPrinter(std::string& _buffer
                                       ): ColumnPrinter(_buffer) {
    // PASS
  }

  void VoidColumnPrinter::reset(const  ColumnVectorBatch&) {
    // PASS
  }

  void VoidColumnPrinter::printRow(uint64_t) {
    writeNull(buffer);
  }

  LongColumnPrinter::LongColumnPrinter(std::string& _buffer
                                       ): ColumnPrinter(_buffer),
                                          data(nullptr) {
    // PASS
  }

  void LongColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  void LongColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeNull(buffer);
    } else {
      char numBuffer[64];
      auto len = snprintf(numBuffer, sizeof(numBuffer), "%" INT64_FORMAT_STRING "d",
               static_cast<int64_t >(data[rowId]));
      writeString(buffer, numBuffer, len);
    }
  }

  DoubleColumnPrinter::DoubleColumnPrinter(std::string& _buffer,
                                           const Type& type
                                           ): ColumnPrinter(_buffer),
                                              data(nullptr),
                                              isFloat(type.getKind() == FLOAT){
    // PASS
  }

  void DoubleColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const DoubleVectorBatch&>(batch).data.data();
  }

  void DoubleColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeNull(buffer);
    } else {
      if (isnan(data[rowId])) {
        writeString(buffer, "\"NaN\"", sizeof("\"NaN\"")-1);
      } else if (isinf(data[rowId])) {
        writeString(buffer, "\"Infinity\"", sizeof("\"Infinity\"")-1);
      } else {
        char numBuffer[64];
        auto len = snprintf(numBuffer, sizeof(numBuffer), isFloat ? "%.7g" : "%.14g",
                 data[rowId]);
        writeString(buffer, numBuffer, len);
      }
    }
  }

  Decimal64ColumnPrinter::Decimal64ColumnPrinter(std::string& _buffer
                                                 ): ColumnPrinter(_buffer),
                                                    data(nullptr),
                                                    scale(0) {
    // PASS
  }

  void Decimal64ColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const Decimal64VectorBatch&>(batch).values.data();
    scale = dynamic_cast<const Decimal64VectorBatch&>(batch).scale;
  }

  std::string toDecimalString(int64_t value, int32_t scale) {
    std::stringstream buffer;
    if (scale == 0) {
      buffer << value;
      return buffer.str();
    }
    std::string sign = "";
    if (value < 0) {
      sign = "-";
      value = -value;
    }
    buffer << value;
    std::string str = buffer.str();
    int32_t len = static_cast<int32_t>(str.length());
    if (len > scale) {
      return sign + str.substr(0, static_cast<size_t>(len - scale)) + "." +
        str.substr(static_cast<size_t>(len - scale),
                   static_cast<size_t>(scale));
    } else if (len == scale) {
      return sign + "0." + str;
    } else {
      std::string result = sign + "0.";
      for(int32_t i=0; i < scale - len; ++i) {
        result += "0";
      }
      return result + str;
    }
  }

  void Decimal64ColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeNull(buffer);
    } else {
      auto decimalString = toDecimalString(data[rowId], scale);
      writeString(buffer, decimalString.c_str(), decimalString.length());
    }
  }

  Decimal128ColumnPrinter::Decimal128ColumnPrinter(std::string& _buffer
                                                   ): ColumnPrinter(_buffer),
                                                      data(nullptr),
                                                      scale(0) {
     // PASS
   }

   void Decimal128ColumnPrinter::reset(const  ColumnVectorBatch& batch) {
     ColumnPrinter::reset(batch);
     data = dynamic_cast<const Decimal128VectorBatch&>(batch).values.data();
     scale = dynamic_cast<const Decimal128VectorBatch&>(batch).scale;
   }

   void Decimal128ColumnPrinter::printRow(uint64_t rowId) {
     if (hasNulls && !notNull[rowId]) {
       writeNull(buffer);
     } else {
       auto decimalString = data[rowId].toDecimalString(scale);
       writeString(buffer, decimalString.c_str(), decimalString.length());
     }
   }

  StringColumnPrinter::StringColumnPrinter(std::string& _buffer
                                           ): ColumnPrinter(_buffer),
                                              start(nullptr),
                                              length(nullptr) {
    // PASS
  }

  void StringColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    start = dynamic_cast<const StringVectorBatch&>(batch).data.data();
    length = dynamic_cast<const StringVectorBatch&>(batch).length.data();
  }

  void StringColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeNull(buffer);
    } else {
      writeQuotedString(buffer, start[rowId], length[rowId]);
    }
  }

  ListColumnPrinter::ListColumnPrinter(std::string& _buffer,
                                       const Type& type
                                       ): ColumnPrinter(_buffer),
                                          offsets(nullptr) {
    elementPrinter = createColumnPrinter(buffer, type.getSubtype(0));
  }

  void ListColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    offsets = dynamic_cast<const ListVectorBatch&>(batch).offsets.data();
    elementPrinter->reset(*dynamic_cast<const ListVectorBatch&>(batch).
                          elements);
  }

  void ListColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeNull(buffer);
    } else {
      writeChar(buffer, '[');
      for(int64_t i=offsets[rowId]; i < offsets[rowId+1]; ++i) {
        if (i != offsets[rowId]) {
          writeChar(buffer, ',');
        }
        elementPrinter->printRow(static_cast<uint64_t>(i));
      }
      writeChar(buffer, ']');
    }
  }

  MapColumnPrinter::MapColumnPrinter(std::string& _buffer,
                                     const Type& type
                                     ): ColumnPrinter(_buffer),
                                        offsets(nullptr) {
    keyPrinter = createColumnPrinter(buffer, type.getSubtype(0));
    elementPrinter = createColumnPrinter(buffer, type.getSubtype(1));
  }

  void MapColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const MapVectorBatch& myBatch = dynamic_cast<const MapVectorBatch&>(batch);
    offsets = myBatch.offsets.data();
    keyPrinter->reset(*myBatch.keys);
    elementPrinter->reset(*myBatch.elements);
  }

void MapColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeNull(buffer);
    } else {
      writeChar(buffer, '{');
      for(int64_t i=offsets[rowId]; i < offsets[rowId+1]; ++i) {
        if (i != offsets[rowId]) {
          writeChar(buffer, ',');
        }
        keyPrinter->printRow(static_cast<uint64_t>(i));
        writeChar(buffer, ':');
        elementPrinter->printRow(static_cast<uint64_t>(i));
      }
      writeChar(buffer, '}');
    }
  }
  
  UnionColumnPrinter::UnionColumnPrinter(std::string& _buffer,
                                           const Type& type
                                         ): ColumnPrinter(_buffer),
                                            tags(nullptr),
                                            offsets(nullptr) {
    for(unsigned int i=0; i < type.getSubtypeCount(); ++i) {
      fieldPrinter.push_back(createColumnPrinter(buffer, type.getSubtype(i)));
    }
  }

  void UnionColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const UnionVectorBatch& unionBatch =
      dynamic_cast<const UnionVectorBatch&>(batch);
    tags = unionBatch.tags.data();
    offsets = unionBatch.offsets.data();
    for(size_t i=0; i < fieldPrinter.size(); ++i) {
      fieldPrinter[i]->reset(*(unionBatch.children[i]));
    }
  }

  void UnionColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeNull(buffer);
    } else {
      writeString(buffer, "{\"tag\":", sizeof("{\"tag\":")-1);
      char numBuffer[64];
      auto len = snprintf(numBuffer, sizeof(numBuffer), "%" INT64_FORMAT_STRING "d",
               static_cast<int64_t>(tags[rowId]));
      writeString(buffer, numBuffer, len);
      writeString(buffer, ",\"value\":", sizeof(",\"value\":")-1);
      fieldPrinter[tags[rowId]]->printRow(offsets[rowId]);
      writeChar(buffer, '}');
    }
  }

  StructColumnPrinter::StructColumnPrinter(std::string& _buffer,
                                           const Type& type
                                           ): ColumnPrinter(_buffer) {
    for(unsigned int i=0; i < type.getSubtypeCount(); ++i) {
      fieldNames.push_back(type.getFieldName(i));
      fieldPrinter.push_back(createColumnPrinter(buffer, type.getSubtype(i)));
    }
  }

  void StructColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const StructVectorBatch& structBatch =
      dynamic_cast<const StructVectorBatch&>(batch);
    for(size_t i=0; i < fieldPrinter.size(); ++i) {
      fieldPrinter[i]->reset(*(structBatch.fields[i]));
    }
  }

  void StructColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeNull(buffer);
    } else {
      writeChar(buffer, '{');
      for(unsigned int i=0; i < fieldPrinter.size(); ++i) {
        if (i != 0) {
          writeChar(buffer, ',');
        }
        writeChar(buffer, '"');
        auto &fieldName = fieldNames[i];
        writeString(buffer, fieldName.c_str(), fieldName.length());
        writeString(buffer, "\":", sizeof("\":")-1);
        fieldPrinter[i]->printRow(rowId);
      }
      writeChar(buffer, '}');
    }
  }

  DateColumnPrinter::DateColumnPrinter(std::string& _buffer
                                       ): ColumnPrinter(_buffer),
                                          data(nullptr) {
    // PASS
  }

  void DateColumnPrinter::printRow(uint64_t rowId) {
    const char* invalidDate = "0000-00-00";
    if (hasNulls && !notNull[rowId]) {
      writeNull(buffer);
    } else {
      const time_t timeValue = data[rowId] * 24 * 60 * 60;
      struct tm tmValue;
      writeChar(buffer, '"');
      if (!gmtime_r(&timeValue, &tmValue)) {
        writeString(buffer, invalidDate, sizeof(invalidDate)-1);
      } else {
        char timeBuffer[11];
        auto len = strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d", &tmValue);
        writeString(buffer, timeBuffer, len);
      }
      writeChar(buffer, '"');
    }
  }

  void DateColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  BooleanColumnPrinter::BooleanColumnPrinter(std::string& _buffer
                                             ): ColumnPrinter(_buffer),
                                                data(nullptr) {
    // PASS
  }

  void BooleanColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeNull(buffer);
    } else {
      if (data[rowId]) {
        writeString(buffer, "true", sizeof("true")-1);
      } else {
        writeString(buffer, "false", sizeof("false")-1);
      }
    }
  }

  void BooleanColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  BinaryColumnPrinter::BinaryColumnPrinter(std::string& _buffer
                                           ): ColumnPrinter(_buffer),
                                              start(nullptr),
                                              length(nullptr) {
    // PASS
  }

  void BinaryColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeNull(buffer);
    } else {
      writeChar(buffer, '[');
      for(int64_t i=0; i < length[rowId]; ++i) {
        if (i != 0) {
          writeChar(buffer, ',');
        }
        char numBuffer[64];
        auto len = snprintf(numBuffer, sizeof(numBuffer), "%d",
                 (static_cast<const int>(start[rowId][i]) & 0xff));
        writeString(buffer, numBuffer, len);
      }
      writeChar(buffer, ']');
    }
  }

  void BinaryColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    start = dynamic_cast<const StringVectorBatch&>(batch).data.data();
    length = dynamic_cast<const StringVectorBatch&>(batch).length.data();
  }

  TimestampColumnPrinter::TimestampColumnPrinter(std::string& _buffer
                                                 ): ColumnPrinter(_buffer),
                                                    seconds(nullptr),
                                                    nanoseconds(nullptr) {
    // PASS
  }

  void TimestampColumnPrinter::printRow(uint64_t rowId) {
    const char* invalidTime = "0000-00-00 00:00:00";
    const int64_t NANO_DIGITS = 9;
    if (hasNulls && !notNull[rowId]) {
      writeNull(buffer);
    } else {
      int64_t nanos = nanoseconds[rowId];
      time_t secs = static_cast<time_t>(seconds[rowId]);
      struct tm tmValue;
      writeChar(buffer, '"');
      if (!gmtime_r(&secs, &tmValue)) {
        writeString(buffer, invalidTime, sizeof(invalidTime)-1);
        writeChar(buffer, '"');
      } else {
        char timeBuffer[20];
        auto len = strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        writeString(buffer, timeBuffer, len);
        writeChar(buffer, '.');
        // remove trailing zeros off the back of the nanos value.
        int64_t zeroDigits = 0;
        if (nanos == 0) {
          zeroDigits = 8;
        } else {
          while (nanos % 10 == 0) {
            nanos /= 10;
            zeroDigits += 1;
          }
        }
        char numBuffer[64];
        len = snprintf(numBuffer, sizeof(numBuffer),
                 "%0*" INT64_FORMAT_STRING "d\"",
                 static_cast<int>(NANO_DIGITS - zeroDigits),
                 static_cast<int64_t >(nanos));
        writeString(buffer, numBuffer, len);
      }
    }
  }

  void TimestampColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const TimestampVectorBatch& ts =
      dynamic_cast<const TimestampVectorBatch&>(batch);
    seconds = ts.data.data();
    nanoseconds = ts.nanoseconds.data();
  }
}
