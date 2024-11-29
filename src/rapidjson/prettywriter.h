// Tencent is pleased to support the open source community by making RapidJSON available.
// 
// Copyright (C) 2015 THL A29 Limited, a Tencent company, and Milo Yip.
//
// Licensed under the MIT License (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
// http://opensource.org/licenses/MIT
//
// Unless required by applicable law or agreed to in writing, software distributed 
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
// CONDITIONS OF ANY KIND, either express or implied. See the License for the 
// specific language governing permissions and limitations under the License.

#ifndef RAPIDJSON_PRETTYWRITER_H_
#define RAPIDJSON_PRETTYWRITER_H_

#include <rapidjson/writer.h>
#include "json/json.h"

#ifdef __GNUC__
RAPIDJSON_DIAG_PUSH
RAPIDJSON_DIAG_OFF(effc++)
#endif

#if defined(__clang__)
RAPIDJSON_DIAG_PUSH
RAPIDJSON_DIAG_OFF(c++98-compat)
#endif

RAPIDJSON_NAMESPACE_BEGIN

//! Combination of PrettyWriter format flags.
/*! \see PrettyWriter::SetFormatOptions
 */
enum PrettyFormatOptions {
    kFormatDefault = 0,         //!< Default pretty formatting.
    kFormatSingleLineArray = 1  //!< Format arrays on a single line.
};

//! Writer with indentation and spacing.
/*!
    \tparam OutputStream Type of output os.
    \tparam SourceEncoding Encoding of source string.
    \tparam TargetEncoding Encoding of output stream.
    \tparam StackAllocator Type of allocator for allocating memory of stack.
*/
template<typename OutputStream, typename SourceEncoding = UTF8<>, typename TargetEncoding = UTF8<>, typename StackAllocator = CrtAllocator, unsigned writeFlags = kWriteDefaultFlags>
class PrettyWriter : public Writer<OutputStream, SourceEncoding, TargetEncoding, StackAllocator, writeFlags> {
public:
    typedef Writer<OutputStream, SourceEncoding, TargetEncoding, StackAllocator, writeFlags> Base;
    typedef typename Base::Ch Ch;

    //! Constructor
    /*! \param os Output stream.
        \param allocator User supplied allocator. If it is null, it will create a private one.
        \param levelDepth Initial capacity of stack.
    */
    explicit PrettyWriter(OutputStream& os, StackAllocator* allocator = 0, size_t levelDepth = Base::kDefaultLevelDepth) : 
        Base(os, allocator, levelDepth), formatOptions_(kFormatDefault), initialLevel(0), curDepth(0), maxDepth(0) {}


    explicit PrettyWriter(StackAllocator* allocator = 0, size_t levelDepth = Base::kDefaultLevelDepth) : 
        Base(allocator, levelDepth), formatOptions_(kFormatDefault), initialLevel(0), curDepth(0), maxDepth(0) {}

#if RAPIDJSON_HAS_CXX11_RVALUE_REFS
    PrettyWriter(PrettyWriter&& rhs) :
        Base(std::forward<PrettyWriter>(rhs)), formatOptions_(rhs.formatOptions_),
        newline_(rhs.newline_), indent_(rhs.indent_), space_(rhs.space_), initialLevel(rhs.initialLevel_) {}
#endif

    //! Set pretty writer formatting options.
    /*! \param options Formatting options.
    */
    PrettyWriter& SetFormatOptions(PrettyFormatOptions options) {
        formatOptions_ = options;
        return *this;
    }
    PrettyWriter& SetNewline(const std::string_view &newline) {
        newline_ = newline;
        return *this;
    }
    PrettyWriter& SetIndent(const std::string_view &indent) {
        indent_ = indent;
        return *this;
    }
    PrettyWriter& SetSpace(const std::string_view &space) {
        space_ = space;
        return *this;
    }
    PrettyWriter& SetInitialLevel(size_t il) {
        initialLevel = il;
        return *this;
    }

    /*! @name Implementation of Handler
        \see Handler
    */
    //@{

    bool Null()                 { PrettyPrefix(kNullType);   return Base::EndValue(Base::WriteNull()); }
    bool Bool(bool b)           { PrettyPrefix(b ? kTrueType : kFalseType); return Base::EndValue(Base::WriteBool(b)); }
    bool Int(int i)             { PrettyPrefix(kNumberType); return Base::EndValue(Base::WriteInt(i)); }
    bool Uint(unsigned u)       { PrettyPrefix(kNumberType); return Base::EndValue(Base::WriteUint(u)); }
    bool Int64(int64_t i64)     { PrettyPrefix(kNumberType); return Base::EndValue(Base::WriteInt64(i64)); }
    bool Uint64(uint64_t u64)   { PrettyPrefix(kNumberType); return Base::EndValue(Base::WriteUint64(u64));  }
    bool Double(double d)       { PrettyPrefix(kNumberType); return Base::EndValue(Base::WriteDouble(d)); }

    bool RawNumber(const Ch* str, SizeType length, bool copy = false) {
        RAPIDJSON_ASSERT(str != 0);
        (void)copy;
        PrettyPrefix(kNumberType);
        return Base::EndValue(Base::WriteDouble(str, length));
    }

    bool String(const Ch* str, SizeType length, bool copy = false) {
        RAPIDJSON_ASSERT(str != 0);
        (void)copy;
        PrettyPrefix(kStringType);
        return Base::EndValue(Base::WriteString(str, length));
    }

#if RAPIDJSON_HAS_STDSTRING
    bool String(const std::basic_string<Ch>& str) {
        return String(str.data(), SizeType(str.size()));
    }
#endif

    size_t GetMaxDepth() {
        return maxDepth;
    }

    bool StartObject() {
        IncrDepth();
        PrettyPrefix(kObjectType);
        new (Base::level_stack_.template Push<typename Base::Level>()) typename Base::Level(false);
        return Base::WriteStartObject();
    }

    bool Key(const Ch* str, SizeType length, bool copy = false) { return String(str, length, copy); }

#if RAPIDJSON_HAS_STDSTRING
    bool Key(const std::basic_string<Ch>& str) {
        return Key(str.data(), SizeType(str.size()));
    }
#endif
	
    bool EndObject(SizeType memberCount = 0) {
        (void)memberCount;
        RAPIDJSON_ASSERT(Base::level_stack_.GetSize() >= sizeof(typename Base::Level)); // not inside an Object
        RAPIDJSON_ASSERT(!Base::level_stack_.template Top<typename Base::Level>()->inArray); // currently inside an Array, not Object
        RAPIDJSON_ASSERT(0 == Base::level_stack_.template Top<typename Base::Level>()->valueCount % 2); // Object has a Key without a Value
       
        bool empty = Base::level_stack_.template Pop<typename Base::Level>(1)->valueCount == 0;

        if (!empty) {
            WriteNewline();
            WriteIndent();
        }
        bool ret = Base::EndValue(Base::WriteEndObject());
        (void)ret;
        RAPIDJSON_ASSERT(ret == true);
        if (Base::level_stack_.Empty()) // end of json text
            Base::Flush();
        DecrDepth();
        return true;
    }

    bool StartArray() {
        IncrDepth();
        PrettyPrefix(kArrayType);
        new (Base::level_stack_.template Push<typename Base::Level>()) typename Base::Level(true);
        return Base::WriteStartArray();
    }

    bool EndArray(SizeType memberCount = 0) {
        (void)memberCount;
        RAPIDJSON_ASSERT(Base::level_stack_.GetSize() >= sizeof(typename Base::Level));
        RAPIDJSON_ASSERT(Base::level_stack_.template Top<typename Base::Level>()->inArray);
        bool empty = Base::level_stack_.template Pop<typename Base::Level>(1)->valueCount == 0;

        if (!empty && !(formatOptions_ & kFormatSingleLineArray)) {
            WriteNewline();
            WriteIndent();
        }
        bool ret = Base::EndValue(Base::WriteEndArray());
        (void)ret;
        RAPIDJSON_ASSERT(ret == true);
        if (Base::level_stack_.Empty()) // end of json text
            Base::Flush();
        DecrDepth();
        return true;
    }

    //@}

    /*! @name Convenience extensions */
    //@{

    //! Simpler but slower overload.
    bool String(const Ch* str) { return String(str, internal::StrLen(str)); }
    bool Key(const Ch* str) { return Key(str, internal::StrLen(str)); }

    //@}

    //! Write a raw JSON value.
    /*!
        For user to write a stringified JSON as a value.

        \param json A well-formed JSON value. It should not contain null character within [0, length - 1] range.
        \param length Length of the json.
        \param type Type of the root of json.
        \note When using PrettyWriter::RawValue(), the result json may not be indented correctly.
    */
    bool RawValue(const Ch* json, size_t length, Type type) {
        RAPIDJSON_ASSERT(json != 0);
        PrettyPrefix(type);
        return Base::EndValue(Base::WriteRawValue(json, length));
    }

protected:
    void PrettyPrefix(Type type) {
        (void)type;
        if (Base::level_stack_.GetSize() != 0) { // this value is not at root
            typename Base::Level* level = Base::level_stack_.template Top<typename Base::Level>();

            if (level->inArray) {
                if (level->valueCount > 0) {
                    Base::os_->Put(','); // add comma if it is not the first element in array
                    if (formatOptions_ & kFormatSingleLineArray)
                        WriteSpace();
                }

                if (!(formatOptions_ & kFormatSingleLineArray)) {
                    WriteNewline();
                    WriteIndent();
                }
            }
            else {  // in object
                if (level->valueCount > 0) {
                    if (level->valueCount % 2 == 0) {
                        Base::os_->Put(',');
                        WriteNewline();
                    }
                    else {
                        Base::os_->Put(':');
                        WriteSpace();
                    }
                }
                else
                    WriteNewline();

                if (level->valueCount % 2 == 0)
                    WriteIndent();
            }
            if (!level->inArray && level->valueCount % 2 == 0)
                RAPIDJSON_ASSERT(type == kStringType);  // if it's in object, then even number should be a name
            level->valueCount++;
        }
        else {
            RAPIDJSON_ASSERT(!Base::hasRoot_);  // Should only has one and only one root.
            Base::hasRoot_ = true;
        }
    }
    void WriteStringView(const std::string_view& v) {
        if (!v.empty()) {
            size_t sz = v.size();
            char *buf = Base::os_->Push(sz);
            v.copy(buf, sz);
        }
    }
    void WriteString(const char *ptr, size_t len, bool noescape) {
        if (noescape) {
            char *p = Base::os_->Push(len + 2);
            p[0] = '"';
            std::memcpy(p + 1, ptr, len);
            p[len + 1] = '"';
        } else {
            Base::WriteString(ptr, len);
        }
    }
    void WriteNewline() { WriteStringView(newline_); }
    void WriteSpace() { WriteStringView(space_); }
    void WriteIndent()  {
        size_t count = initialLevel + (Base::level_stack_.GetSize() / sizeof(typename Base::Level));
        for (size_t i = 0; i < count; ++i) WriteStringView(indent_);
    }

public:
    //
    // Accelerated write when there's definitely no format
    //
    template<typename JValue>
    void FastWrite(JValue &value, size_t *max_depth) {
        *max_depth = 0;
        FastWrite_internal(value, 0, max_depth);
    }

    PrettyFormatOptions formatOptions_;
    std::string_view newline_;
    std::string_view indent_;
    std::string_view space_;
    size_t initialLevel;

private:
    // Prohibit copy constructor & assignment operator.
    PrettyWriter(const PrettyWriter&);
    PrettyWriter& operator=(const PrettyWriter&);
    size_t curDepth;
    size_t maxDepth;

    void IncrDepth() {
        curDepth++;
        if (curDepth > maxDepth) maxDepth = curDepth;
    }

    void DecrDepth() {
        RAPIDJSON_ASSERT(curDepth > 0);
        curDepth--;
    }

    template<typename JValue>
    void FastWrite_internal(JValue &value, const size_t level, size_t *max_depth) {
        if (level > *max_depth) *max_depth = level;

        bool firstElement;
        switch (value.GetType()) {
            case kStringType:
                WriteString(value.GetString(), value.GetStringLength(), value.IsNoescape());
                break;
            case kNullType:
                Base::WriteNull();
                break;
            case kFalseType:
                Base::WriteBool(false);
                break;
            case kTrueType:
                Base::WriteBool(true);
                break;
            case kObjectType:
                Base::os_->Put('{');
                firstElement = true;
                for (typename JValue::ConstMemberIterator m = value.MemberBegin(); m != value.MemberEnd(); ++m) {
                    if (!firstElement) {
                        Base::os_->Put(',');
                    } else {
                        firstElement = false;
                    }
                    WriteString(m->name.GetString(), m->name.GetStringLength(), m->name.IsNoescape());
                    Base::os_->Put(':');
                    FastWrite_internal(m->value, level + 1, max_depth);
                }
                Base::os_->Put('}');
                break;
            case kArrayType:
                Base::os_->Put('[');
                firstElement = true;
                for (typename JValue::ConstValueIterator v = value.Begin(); v != value.End(); ++v) {
                    if (!firstElement) {
                        Base::os_->Put(',');
                    } else {
                        firstElement = false;
                    }
                    FastWrite_internal(*v, level + 1, max_depth);
                }
                Base::os_->Put(']');
                break;
            default:
                RAPIDJSON_ASSERT(value.GetType() == kNumberType);
                if (value.IsDouble()) { 
                    Base::WriteDouble(value.GetDoubleString(), value.GetDoubleStringLength());
                }
                else if (value.IsInt())       Base::WriteInt(value.GetInt());
                else if (value.IsUint())      Base::WriteUint(value.GetUint());
                else if (value.IsInt64())     Base::WriteInt64(value.GetInt64());
                else                          Base::WriteUint64(value.GetUint64());
                break;
        }
    }

};

RAPIDJSON_NAMESPACE_END

#if defined(__clang__)
RAPIDJSON_DIAG_POP
#endif

#ifdef __GNUC__
RAPIDJSON_DIAG_POP
#endif

#endif // RAPIDJSON_RAPIDJSON_H_
