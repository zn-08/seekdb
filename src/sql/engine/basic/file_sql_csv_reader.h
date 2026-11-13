/*
 * Copyright (c) 2026 OceanBase.
 * Licensed under the Apache License, Version 2.0.
 */

#pragma once

#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <sys/stat.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace oceanbase
{
namespace sql
{

// A bounded-memory CSV reader shared by schema discovery and the scan operator.
// The caller must canonicalize the path and enforce secure_file_priv before open().
class FileSQLCsvReader
{
public:
  enum class Status { ROW, END, ERROR };
  enum class Type { NULL_ONLY, BOOLEAN, BIGINT, DOUBLE, DATE, DATETIME, VARCHAR };

  struct Column
  {
    std::string name;
    Type type = Type::NULL_ONLY;
    bool nullable = false;
  };

  FileSQLCsvReader() = default;
  FileSQLCsvReader(const FileSQLCsvReader &) = delete;
  FileSQLCsvReader &operator=(const FileSQLCsvReader &) = delete;

  bool open(const std::string &path, bool header = true)
  {
    path_ = path;
    has_header_ = header;
    error_.clear();
    columns_.clear();
    row_number_ = 0;
    stream_.close();
    FileStat stat_buf;
    if (file_stat(path_, stat_buf) != 0 || stat_buf.st_size < 0
        || static_cast<uintmax_t>(stat_buf.st_size) > MAX_FILE_SIZE) {
      return fail("file is inaccessible or exceeds the size limit");
    }
    expected_size_ = static_cast<uintmax_t>(stat_buf.st_size);
    expected_mtime_ = stat_buf.st_mtime;
    stream_.open(path_, std::ios::binary);
    if (!stream_.is_open()) {
      return fail("cannot open CSV file");
    }
    std::vector<std::string> row;
    Status status = next(row);
    if (status == Status::ERROR) {
      return false;
    } else if (status == Status::END) {
      return fail("CSV file has no schema row");
    }
    if (row.empty() || row.size() > MAX_COLUMNS) {
      return fail("CSV column count is invalid");
    }
    std::unordered_map<std::string, unsigned> occurrences;
    for (size_t i = 0; i < row.size(); ++i) {
      Column col;
      if (has_header_) {
        const std::string base = row[i].empty() ? "column" + std::to_string(i + 1) : row[i];
        const unsigned occurrence = ++occurrences[base];
        col.name = occurrence == 1 ? base : base + "__" + std::to_string(occurrence);
      } else {
        col.name = "column" + std::to_string(i + 1);
      }
      columns_.push_back(std::move(col));
    }
    if (!has_header_) {
      infer_row(row);
    }
    while ((status = next(row)) == Status::ROW) {
      if (!infer_row(row)) {
        return false;
      }
    }
    if (status == Status::ERROR || !unchanged()) {
      return false;
    }
    for (Column &col : columns_) {
      if (col.type == Type::NULL_ONLY) {
        col.type = Type::VARCHAR;
        col.nullable = true;
      }
    }
    return rescan();
  }

  bool rescan()
  {
    if (!unchanged()) {
      return false;
    }
    stream_.clear();
    stream_.seekg(0);
    row_number_ = 0;
    if (!stream_) {
      return fail("cannot rewind CSV file");
    }
    if (has_header_) {
      std::vector<std::string> header;
      if (next(header) != Status::ROW) {
        return fail("CSV header changed");
      }
    }
    return true;
  }

  void close()
  {
    stream_.close();
    columns_.clear();
    path_.clear();
  }

  Status next(std::vector<std::string> &row)
  {
    row.clear();
    if (!stream_.is_open()) {
      fail("CSV file is not open");
      return Status::ERROR;
    }
    std::string field;
    bool quoted = false;
    bool after_quote = false;
    bool at_field_start = true;
    bool saw_data = false;
    size_t row_bytes = 0;
    for (;;) {
      const int c = stream_.get();
      if (c == EOF) {
        if (stream_.bad()) {
          fail("CSV read failed");
          return Status::ERROR;
        }
        if (quoted) {
          fail("unterminated quoted CSV field");
          return Status::ERROR;
        }
        if (!saw_data && row.empty() && field.empty()) {
          return Status::END;
        }
        row.push_back(std::move(field));
        ++row_number_;
        return Status::ROW;
      }
      saw_data = true;
      if (++row_bytes > MAX_ROW_BYTES) {
        fail("CSV row exceeds the size limit");
        return Status::ERROR;
      }
      if (quoted) {
        if (c == '"') {
          if (stream_.peek() == '"') {
            stream_.get();
            field.push_back('"');
            ++row_bytes;
          } else {
            quoted = false;
            after_quote = true;
          }
        } else if (c == '\r') {
          if (stream_.peek() == '\n') {
            stream_.get();
            ++row_bytes;
          }
          field.push_back('\n');
        } else {
          field.push_back(static_cast<char>(c));
        }
      } else if (c == ',' || c == '\n' || c == '\r') {
        row.push_back(std::move(field));
        field.clear();
        after_quote = false;
        at_field_start = true;
        if (row.size() > MAX_COLUMNS) {
          fail("CSV column count exceeds the limit");
          return Status::ERROR;
        }
        if (c == '\r' && stream_.peek() == '\n') {
          stream_.get();
        }
        if (c != ',') {
          ++row_number_;
          return Status::ROW;
        }
      } else if (c == '"' && at_field_start) {
        quoted = true;
        at_field_start = false;
      } else if (after_quote) {
        fail("characters after quoted CSV field");
        return Status::ERROR;
      } else if (c == '"') {
        fail("unexpected quote in CSV field");
        return Status::ERROR;
      } else {
        field.push_back(static_cast<char>(c));
        at_field_start = false;
      }
    }
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
        || static_cast<uintmax_t>(stat_buf.st_size) != expected_size_) {
      return fail("CSV file changed during scan");
    }
    return stat_buf.st_mtime == expected_mtime_ ? true : fail("CSV file changed during scan");
  }

  bool infer_row(const std::vector<std::string> &row)
  {
    if (row.size() != columns_.size()) {
      return fail("CSV row has an unexpected number of fields");
    }
    for (size_t i = 0; i < row.size(); ++i) {
      const Type observed = infer_type(row[i]);
      Column &col = columns_[i];
      if (observed == Type::NULL_ONLY) {
        col.nullable = true;
      } else if (col.type == Type::NULL_ONLY) {
        col.type = observed;
      } else if (col.type != observed) {
        if ((col.type == Type::BIGINT && observed == Type::DOUBLE)
            || (col.type == Type::DOUBLE && observed == Type::BIGINT)) {
          col.type = Type::DOUBLE;
        } else if ((col.type == Type::DATE && observed == Type::DATETIME)
                   || (col.type == Type::DATETIME && observed == Type::DATE)) {
          col.type = Type::DATETIME;
        } else {
          col.type = Type::VARCHAR;
        }
      }
    }
    return true;
  }

  static bool valid_date(const std::string &text)
  {
    if (text.size() != 10 || text[4] != '-' || text[7] != '-') {
      return false;
    }
    for (size_t i = 0; i < text.size(); ++i) {
      if (i != 4 && i != 7 && (text[i] < '0' || text[i] > '9')) {
        return false;
      }
    }
    const int year = std::atoi(text.substr(0, 4).c_str());
    const int month = std::atoi(text.substr(5, 2).c_str());
    const int day = std::atoi(text.substr(8, 2).c_str());
    if (year < 1 || month < 1 || month > 12) {
      return false;
    }
    static constexpr int days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const bool leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    return day >= 1 && day <= days[month] + (month == 2 && leap ? 1 : 0);
  }

  static Type infer_type(const std::string &text)
  {
    if (text.empty()) return Type::NULL_ONLY;
    if (text == "true" || text == "false" || text == "TRUE" || text == "FALSE") return Type::BOOLEAN;
    int64_t integer = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), integer);
    if (parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size()) return Type::BIGINT;
    char *end = nullptr;
    errno = 0;
    const double real = std::strtod(text.c_str(), &end);
    if (errno == 0 && end == text.c_str() + text.size() && std::isfinite(real)) return Type::DOUBLE;
    if (valid_date(text)) return Type::DATE;
    if (text.size() == 19 && (text[10] == ' ' || text[10] == 'T')
        && text[13] == ':' && text[16] == ':' && valid_date(text.substr(0, 10))) {
      for (const size_t i : {size_t{11}, size_t{12}, size_t{14}, size_t{15}, size_t{17}, size_t{18}}) {
        if (text[i] < '0' || text[i] > '9') return Type::VARCHAR;
      }
      const auto hour = std::atoi(text.substr(11, 2).c_str());
      const auto minute = std::atoi(text.substr(14, 2).c_str());
      const auto second = std::atoi(text.substr(17, 2).c_str());
      if (hour < 24 && minute < 60 && second < 60) return Type::DATETIME;
    }
    return Type::VARCHAR;
  }

  std::ifstream stream_;
  std::string path_;
  std::string error_;
  std::vector<Column> columns_;
  int64_t expected_mtime_ = 0;
  uintmax_t expected_size_ = 0;
  size_t row_number_ = 0;
  bool has_header_ = true;
};

} // namespace sql
} // namespace oceanbase
