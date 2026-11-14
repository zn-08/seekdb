/*
 * Copyright (c) 2026 OceanBase.
 * Licensed under the Apache License, Version 2.0.
 */

#pragma once

#include "sql/engine/basic/file_sql_csv_reader.h"
#include <rapidjson/document.h>
#include <climits>
#include <iomanip>
#include <limits>
#include <locale>
#include <sstream>
#include <unordered_map>

namespace oceanbase
{
namespace sql
{

// JSONL schema discovery reads every line but retains only the union of keys.
// Execution reopens the file and materializes at most one flat object at a time.
class FileSQLJsonlReader
{
public:
  using Status = FileSQLCsvReader::Status;
  using Column = FileSQLCsvReader::Column;
  using Type = FileSQLCsvReader::Type;

  struct Cell
  {
    std::string text;
    bool is_null = true;
  };

  FileSQLJsonlReader() = default;
  FileSQLJsonlReader(const FileSQLJsonlReader &) = delete;
  FileSQLJsonlReader &operator=(const FileSQLJsonlReader &) = delete;

  bool open(const std::string &path)
  {
    close();
    path_ = path;
    error_.clear();
    FileStat stat_buf;
    if (file_stat(path_, stat_buf) != 0 || stat_buf.st_size < 0
        || static_cast<uintmax_t>(stat_buf.st_size) > MAX_FILE_SIZE) {
      return fail("JSONL file is inaccessible or exceeds the size limit");
    }
    expected_size_ = static_cast<uintmax_t>(stat_buf.st_size);
    expected_mtime_ = stat_buf.st_mtime;
    stream_.open(path_, std::ios::binary);
    if (!stream_.is_open()) {
      return fail("cannot open JSONL file");
    }
    std::unordered_map<std::string, size_t> key_index;
    rapidjson::Document document;
    Status status;
    while ((status = parse_next(document)) == Status::ROW) {
      std::vector<bool> seen(columns_.size(), false);
      for (auto member = document.MemberBegin(); member != document.MemberEnd(); ++member) {
        const std::string key(member->name.GetString(), member->name.GetStringLength());
        if (key.empty() || key.find('\0') != std::string::npos) {
          return fail("JSONL object has an invalid key");
        }
        const Type observed = value_type(member->value);
        if (observed == Type::NULL_ONLY && !member->value.IsNull()) {
          return fail("nested JSON values are unsupported");
        }
        auto found = key_index.find(key);
        if (found == key_index.end()) {
          if (columns_.size() >= MAX_COLUMNS) {
            return fail("JSONL column count exceeds the limit");
          }
          Column column;
          column.name = key;
          column.type = observed;
          column.nullable = row_number_ > 1 || observed == Type::NULL_ONLY;
          key_index.emplace(key, columns_.size());
          columns_.push_back(std::move(column));
          seen.push_back(true);
        } else {
          const size_t index = found->second;
          if (seen[index]) return fail("duplicate JSONL object key");
          seen[index] = true;
          Column &column = columns_[index];
          if (observed == Type::NULL_ONLY) {
            column.nullable = true;
          } else {
            column.type = FileSQLCsvReader::promote_type(column.type, observed);
          }
        }
      }
      for (size_t i = 0; i < seen.size(); ++i) {
        if (!seen[i]) columns_[i].nullable = true;
      }
    }
    if (status == Status::ERROR || columns_.empty() || !unchanged()) {
      return false;
    }
    for (Column &column : columns_) {
      if (column.type == Type::NULL_ONLY) {
        column.type = Type::VARCHAR;
        column.nullable = true;
      }
    }
    return rescan();
  }

  bool rescan()
  {
    if (!unchanged()) return false;
    stream_.clear();
    stream_.seekg(0);
    row_number_ = 0;
    return stream_.good() ? true : fail("cannot rewind JSONL file");
  }

  Status next(std::vector<Cell> &row)
  {
    row.clear();
    rapidjson::Document document;
    const Status status = parse_next(document);
    if (status != Status::ROW) return status;
    row.resize(columns_.size());
    std::unordered_map<std::string, size_t> key_index;
    for (size_t i = 0; i < columns_.size(); ++i) key_index.emplace(columns_[i].name, i);
    for (auto member = document.MemberBegin(); member != document.MemberEnd(); ++member) {
      const std::string key(member->name.GetString(), member->name.GetStringLength());
      const auto found = key_index.find(key);
      if (found == key_index.end()) {
        fail("JSONL schema changed during scan");
        return Status::ERROR;
      }
      Cell &cell = row[found->second];
      if (member->value.IsNull()) {
        cell.is_null = true;
      } else if (!value_text(member->value, cell.text)) {
        fail("nested JSON values are unsupported");
        return Status::ERROR;
      } else {
        cell.is_null = false;
      }
    }
    return Status::ROW;
  }

  void close()
  {
    stream_.close();
    columns_.clear();
    path_.clear();
    row_number_ = 0;
  }

  const std::vector<Column> &columns() const { return columns_; }
  const std::string &error() const { return error_; }
  size_t row_number() const { return row_number_; }

private:
#ifdef _WIN32
  using FileStat = struct _stat64;
  static int file_stat(const std::string &path, FileStat &out) { return ::_stat64(path.c_str(), &out); }
#else
  using FileStat = struct stat;
  static int file_stat(const std::string &path, FileStat &out) { return ::stat(path.c_str(), &out); }
#endif
  static constexpr size_t MAX_COLUMNS = 4096;
  static constexpr size_t MAX_ROW_BYTES = 16 * 1024 * 1024;
  static constexpr uintmax_t MAX_FILE_SIZE = uintmax_t{1} << 40;

  bool fail(const char *message)
  {
    error_ = message;
    return false;
  }

  bool unchanged()
  {
    FileStat stat_buf;
    if (file_stat(path_, stat_buf) != 0 || stat_buf.st_size < 0
        || static_cast<uintmax_t>(stat_buf.st_size) != expected_size_
        || stat_buf.st_mtime != expected_mtime_) {
      return fail("JSONL file changed during scan");
    }
    return true;
  }

  Status parse_next(rapidjson::Document &document)
  {
    std::string line;
    if (!std::getline(stream_, line)) {
      if (stream_.bad()) {
        fail("JSONL read failed");
        return Status::ERROR;
      }
      return Status::END;
    }
    ++row_number_;
    if (line.size() > MAX_ROW_BYTES) {
      fail("JSONL row exceeds the size limit");
      return Status::ERROR;
    }
    if (!line.empty() && line.back() == '\r') line.pop_back();
    document.Parse<rapidjson::kParseValidateEncodingFlag>(line.c_str(), line.size());
    if (document.HasParseError() || !document.IsObject()) {
      fail("invalid flat JSONL object");
      return Status::ERROR;
    }
    return Status::ROW;
  }

  static Type value_type(const rapidjson::Value &value)
  {
    if (value.IsNull()) return Type::NULL_ONLY;
    if (value.IsBool()) return Type::BOOLEAN;
    if (value.IsInt64()) return Type::BIGINT;
    if (value.IsUint64()) return value.GetUint64() <= static_cast<uint64_t>(INT64_MAX)
                                  ? Type::BIGINT : Type::VARCHAR;
    if (value.IsNumber()) return Type::DOUBLE;
    if (value.IsString()) return FileSQLCsvReader::infer_type(
        std::string(value.GetString(), value.GetStringLength()));
    return Type::NULL_ONLY;
  }

  static bool value_text(const rapidjson::Value &value, std::string &out)
  {
    if (value.IsBool()) {
      out = value.GetBool() ? "1" : "0";
    } else if (value.IsInt64()) {
      out = std::to_string(value.GetInt64());
    } else if (value.IsUint64()) {
      out = std::to_string(value.GetUint64());
    } else if (value.IsNumber()) {
      std::ostringstream stream;
      stream.imbue(std::locale::classic());
      stream << std::setprecision(std::numeric_limits<double>::max_digits10) << value.GetDouble();
      out = stream.str();
    } else if (value.IsString()) {
      out.assign(value.GetString(), value.GetStringLength());
    } else {
      return false;
    }
    return true;
  }

  std::ifstream stream_;
  std::string path_;
  std::string error_;
  std::vector<Column> columns_;
  uintmax_t expected_size_ = 0;
  int64_t expected_mtime_ = 0;
  size_t row_number_ = 0;
};

} // namespace sql
} // namespace oceanbase
