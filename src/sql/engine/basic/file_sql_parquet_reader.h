/*
 * Copyright (c) 2026 OceanBase.
 * Licensed under the Apache License, Version 2.0.
 */

#pragma once

#include <algorithm>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <string>
#include <sys/stat.h>
#include <utility>
#include <vector>

namespace oceanbase { namespace sql {

// A small, bounded reader for flat Parquet columns. The SQL resolver and the
// executor both use the footer schema; the executor decodes one row group at a
// time. Path authorization is the caller's responsibility.
class FileSQLParquetReader
{
public:
  enum class Status { ROW, END, ERROR };
  enum class Type { BOOLEAN, BIGINT, DOUBLE, DATE, DATETIME, VARCHAR };
  struct Column { std::string name; Type type; bool nullable; };

  bool open(const std::string &path)
  {
    close();
    path_ = path;
    in_.open(path, std::ios::binary);
    if (!in_) return fail("cannot open Parquet file");
    in_.seekg(0, std::ios::end);
    const std::streamoff size = in_.tellg();
    if (size < 12 || size > static_cast<std::streamoff>(MAX_FILE_SIZE))
      return fail("invalid Parquet file size");
    file_size_ = static_cast<uint64_t>(size);
    FileStat stat_buf;
    if (file_stat(path, stat_buf) != 0 || stat_buf.st_size != size)
      return fail("Parquet file changed during open");
    file_mtime_ = stat_buf.st_mtime;
    char footer[8];
    in_.seekg(size - 8);
    if (!in_.read(footer, 8) || std::memcmp(footer + 4, "PAR1", 4) != 0)
      return fail("invalid Parquet footer");
    const uint32_t meta_size = u32(reinterpret_cast<const uint8_t *>(footer));
    if (meta_size == 0 || meta_size > MAX_METADATA || meta_size + 12 > file_size_)
      return fail("invalid Parquet metadata length");
    std::string metadata(meta_size, '\0');
    in_.seekg(size - 8 - meta_size);
    if (!in_.read(&metadata[0], meta_size)) return fail("cannot read Parquet metadata");
    Cursor c(metadata);
    if (!parse_file(c) || columns_.empty() || groups_.empty())
      return fail("unsupported or damaged Parquet metadata");
    if (c.pos > c.size) return fail("invalid Parquet metadata");
    projected_.assign(columns_.size(), true);
    if (!unchanged()) return false;
    return rescan();
  }

  bool rescan()
  {
    if (!in_.is_open()) return fail("Parquet file is closed");
    if (!unchanged()) return false;
    group_index_ = 0;
    row_index_ = 0;
    streams_.clear();
    return true;
  }

  bool project(const std::vector<size_t> &indexes)
  {
    projected_.assign(columns_.size(), false);
    for (size_t index : indexes) {
      if (index >= columns_.size()) return fail("invalid Parquet projection");
      projected_[index] = true;
    }
    return rescan();
  }

  void close()
  {
    in_.close();
    columns_.clear();
    projected_.clear();
    logical_.clear();
    physical_.clear();
    groups_.clear();
    streams_.clear();
    path_.clear();
    error_.clear();
    group_index_ = row_index_ = 0;
  }

  Status next(std::vector<std::string> &row, std::vector<bool> &nulls)
  {
    row.clear(); nulls.clear();
    while (group_index_ < groups_.size() && row_index_ >= groups_[group_index_].rows) {
      ++group_index_;
      row_index_ = 0;
      streams_.clear();
    }
    if (group_index_ >= groups_.size()) return Status::END;
    if (streams_.empty() && !open_group(groups_[group_index_])) return Status::ERROR;
    for (size_t i = 0; i < streams_.size(); ++i) {
      if (!projected_[i]) { row.emplace_back(); nulls.push_back(true); continue; }
      ColumnStream &stream = streams_[i];
      if (stream.cell_index == stream.cells.size() && !load_page(i)) return Status::ERROR;
      if (stream.cell_index >= stream.cells.size()) {
        fail("short Parquet column"); return Status::ERROR;
      }
      const Cell &cell = stream.cells[stream.cell_index++];
      row.push_back(cell.text);
      nulls.push_back(cell.is_null);
      ++stream.read;
    }
    ++row_index_;
    return Status::ROW;
  }

  const std::vector<Column> &columns() const { return columns_; }
  const std::string &error() const { return error_; }

private:
#ifdef _WIN32
  using FileStat = struct _stat64;
  static int file_stat(const std::string &path, FileStat &out) { return ::_stat64(path.c_str(), &out); }
#else
  using FileStat = struct stat;
  static int file_stat(const std::string &path, FileStat &out) { return ::stat(path.c_str(), &out); }
#endif
  static constexpr uint64_t MAX_FILE_SIZE = 1ULL << 32;
  static constexpr uint32_t MAX_METADATA = 16U << 20;
  static constexpr uint32_t MAX_PAGE = 64U << 20;
  static constexpr uint32_t MAX_COLUMNS = 1024;
  struct Cell { std::string text; bool is_null = false; };
  struct Chunk { int physical = -1; int codec = -1; uint64_t offset = 0;
                 uint64_t dictionary_offset = 0; uint64_t count = 0; };
  struct Group { uint64_t rows = 0; std::vector<Chunk> chunks; };
  struct ColumnStream { Chunk chunk; uint64_t offset = 0; uint64_t read = 0;
                        std::vector<Cell> dictionary; std::vector<Cell> cells;
                        size_t cell_index = 0; };
  struct Page { int type = -1; int encoding = -1; int def_encoding = -1;
                uint32_t compressed = 0; uint32_t uncompressed = 0;
                uint32_t values = 0; uint32_t def_bytes = 0; bool is_compressed = true; };
  struct Cursor {
    const uint8_t *data; size_t size; size_t pos = 0;
    explicit Cursor(const std::string &s)
      : data(reinterpret_cast<const uint8_t *>(s.data())), size(s.size()) {}
    Cursor(const uint8_t *p, size_t n) : data(p), size(n) {}
    bool byte(uint8_t &v) { if (pos >= size) return false; v = data[pos++]; return true; }
    bool var(uint64_t &v) {
      v = 0;
      for (int i = 0; i < 10; ++i) {
        uint8_t b;
        if (!byte(b) || (i == 9 && b > 1)) return false;
        v |= uint64_t(b & 127) << (7 * i);
        if (!(b & 128)) return true;
      }
      return false;
    }
    bool sint(int64_t &v) { uint64_t n; if (!var(n)) return false;
      v = static_cast<int64_t>((n >> 1) ^ (uint64_t(0) - (n & 1))); return true; }
    bool bytes(std::string &s) { uint64_t n; if (!var(n) || n > size - pos) return false;
      s.assign(reinterpret_cast<const char *>(data + pos), static_cast<size_t>(n));
      pos += static_cast<size_t>(n); return true; }
    bool field(int &last, int &id, uint8_t &type) {
      uint8_t h; if (!byte(h)) return false;
      type = h & 15;
      if (type == 0) { id = 0; return true; }
      const int delta = h >> 4;
      if (delta) id = last + delta;
      else { int64_t n; if (!sint(n) || n <= 0 || n > 32767) return false; id = int(n); }
      last = id; return true;
    }
  };

  static uint32_t u32(const uint8_t *p) {
    return uint32_t(p[0]) | (uint32_t(p[1]) << 8) |
           (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24);
  }
  static uint64_t u64(const uint8_t *p) {
    return uint64_t(u32(p)) | (uint64_t(u32(p + 4)) << 32);
  }
  static bool list(Cursor &c, uint8_t &type, uint64_t &n) {
    uint8_t h; if (!c.byte(h)) return false;
    type = h & 15; n = h >> 4;
    return n != 15 || c.var(n);
  }
  static bool skip(Cursor &c, uint8_t type, int depth = 0) {
    if (depth > 32) return false;
    if (type == 1 || type == 2) return true;
    if (type == 3) { uint8_t b; return c.byte(b); }
    if (type >= 4 && type <= 6) { uint64_t n; return c.var(n); }
    if (type == 7) { if (c.size - c.pos < 8) return false; c.pos += 8; return true; }
    if (type == 8) { std::string s; return c.bytes(s); }
    if (type == 9 || type == 10) {
      uint8_t element; uint64_t n;
      if (!list(c, element, n) || n > 1000000) return false;
      for (uint64_t i = 0; i < n; ++i) if (!skip(c, element, depth + 1)) return false;
      return true;
    }
    if (type == 11) {
      uint64_t n; if (!c.var(n) || n > 1000000) return false;
      if (n) { uint8_t h; if (!c.byte(h)) return false;
        for (uint64_t i = 0; i < n; ++i)
          if (!skip(c, h >> 4, depth + 1) || !skip(c, h & 15, depth + 1)) return false;
      }
      return true;
    }
    if (type == 12) {
      int last = 0, id; uint8_t field_type;
      do { if (!c.field(last, id, field_type)) return false;
        if (field_type && !skip(c, field_type, depth + 1)) return false;
      } while (field_type);
      return true;
    }
    return false;
  }
  static bool integer(Cursor &c, uint8_t type, int64_t &v) {
    return (type == 4 || type == 5 || type == 6) && c.sint(v);
  }
  static bool string(Cursor &c, uint8_t type, std::string &v) {
    return type == 8 && c.bytes(v);
  }
  static bool structs(Cursor &c, uint8_t type, uint64_t &n) {
    uint8_t elem;
    return type == 9 && list(c, elem, n) && elem == 12 && n <= MAX_COLUMNS * 1024;
  }
  static bool parse_time_unit(Cursor &c, int &logical) {
    int last = 0, id; uint8_t t;
    do {
      if (!c.field(last, id, t)) return false;
      if (!t) break;
      if (t == 12 && id >= 1 && id <= 3) {
        logical = id == 1 ? 9 : id == 2 ? 10 : 11;
      }
      if (!skip(c, t)) return false;
    } while (true);
    return true;
  }
  static bool parse_logical_type(Cursor &c, int &logical) {
    int last = 0, id; uint8_t t;
    do {
      if (!c.field(last, id, t)) return false;
      if (!t) break;
      if (id == 6 && t == 12) {
        logical = 6;
        if (!skip(c, t)) return false;
      } else if (id == 8 && t == 12) {
        int inner_last = 0, inner_id; uint8_t inner_t;
        do {
          if (!c.field(inner_last, inner_id, inner_t)) return false;
          if (!inner_t) break;
          if (inner_id == 2 && inner_t == 12) {
            if (!parse_time_unit(c, logical)) return false;
          } else if (!skip(c, inner_t)) return false;
        } while (true);
      } else if (!skip(c, t)) return false;
    } while (true);
    return true;
  }

  bool parse_schema_element(Cursor &c, Column &out, int &children, int &physical,
                            int &logical) {
    int last = 0, id; uint8_t t; int64_t v; children = 0; physical = -1; logical = -1;
    out = Column{"", Type::VARCHAR, false};
    do {
      if (!c.field(last, id, t)) return false;
      if (t == 0) break;
      if (id == 1 && integer(c, t, v)) physical = int(v);
      else if (id == 3 && integer(c, t, v)) out.nullable = (v == 1);
      else if (id == 4 && string(c, t, out.name)) {}
      else if (id == 5 && integer(c, t, v)) children = int(v);
      else if (id == 6 && integer(c, t, v)) logical = int(v);
      else if (id == 10 && t == 12) {
        if (!parse_logical_type(c, logical)) return false;
      }
      else if (!skip(c, t)) return false;
    } while (true);
    if (logical == 6) out.type = Type::DATE;
    else if (logical == 9 || logical == 10 || logical == 11) out.type = Type::DATETIME;
    else if (physical == 0) out.type = Type::BOOLEAN;
    else if (physical == 1 || physical == 2) out.type = Type::BIGINT;
    else if (physical == 4 || physical == 5) out.type = Type::DOUBLE;
    else out.type = Type::VARCHAR;
    return !out.name.empty() && children >= 0;
  }
  bool parse_chunk_meta(Cursor &c, Chunk &out) {
    int last = 0, id; uint8_t t; int64_t v;
    do {
      if (!c.field(last, id, t)) return false;
      if (!t) break;
      if (id == 1 && integer(c, t, v)) out.physical = int(v);
      else if (id == 4 && integer(c, t, v)) out.codec = int(v);
      else if (id == 5 && integer(c, t, v)) out.count = uint64_t(v);
      else if (id == 9 && integer(c, t, v)) out.offset = uint64_t(v);
      else if (id == 11 && integer(c, t, v)) out.dictionary_offset = uint64_t(v);
      else if (!skip(c, t)) return false;
    } while (true);
    return out.physical >= 0 && out.codec >= 0 && out.count <= 100000000;
  }
  bool parse_chunk(Cursor &c, Chunk &out) {
    int last = 0, id; uint8_t t; bool has_meta = false;
    do {
      if (!c.field(last, id, t)) return false;
      if (!t) break;
      if (id == 3 && t == 12) { if (!parse_chunk_meta(c, out)) return false; has_meta = true; }
      else if (!skip(c, t)) return false;
    } while (true);
    return has_meta;
  }
  bool parse_group(Cursor &c, Group &out) {
    int last = 0, id; uint8_t t; int64_t v;
    do {
      if (!c.field(last, id, t)) return false;
      if (!t) break;
      if (id == 1) {
        uint64_t n; if (!structs(c, t, n) || n > MAX_COLUMNS) return false;
        for (uint64_t i = 0; i < n; ++i) {
          Chunk chunk; if (!parse_chunk(c, chunk)) return false;
          out.chunks.push_back(chunk);
        }
      } else if (id == 3 && integer(c, t, v)) out.rows = uint64_t(v);
      else if (!skip(c, t)) return false;
    } while (true);
    return out.rows <= 100000000 && out.chunks.size() == columns_.size();
  }
  bool parse_file(Cursor &c) {
    int last = 0, id; uint8_t t; int64_t v; uint64_t total_rows = 0;
    do {
      if (!c.field(last, id, t)) return false;
      if (!t) break;
      if (id == 2) {
        uint64_t n; if (!structs(c, t, n) || n < 2 || n > MAX_COLUMNS + 1) return false;
        for (uint64_t i = 0; i < n; ++i) {
          Column column; int children, physical, logical;
          if (!parse_schema_element(c, column, children, physical, logical)) return false;
          if (i == 0) { if (children != int(n - 1)) return false; }
          else { if (children != 0 || physical < 0) return false;
            columns_.push_back(column); physical_.push_back(physical);
            logical_.push_back(logical); }
        }
      } else if (id == 3 && integer(c, t, v)) total_rows = uint64_t(v);
      else if (id == 4) {
        uint64_t n; if (!structs(c, t, n) || n > 100000) return false;
        for (uint64_t i = 0; i < n; ++i) {
          Group group; if (!parse_group(c, group)) return false;
          groups_.push_back(std::move(group));
        }
      } else if (!skip(c, t)) return false;
    } while (true);
    uint64_t sum = 0;
    for (const auto &group : groups_) sum += group.rows;
    return sum == total_rows;
  }

  static bool snappy(const std::string &src, uint32_t expected, std::string &dst) {
    Cursor c(src); uint64_t length;
    if (!c.var(length) || length != expected || length > MAX_PAGE) return false;
    dst.clear(); dst.reserve(static_cast<size_t>(length));
    while (c.pos < c.size && dst.size() < length) {
      uint8_t tag; if (!c.byte(tag)) return false;
      const int kind = tag & 3;
      if (kind == 0) {
        uint64_t n = tag >> 2;
        if (n < 60) ++n;
        else { const int width = int(n - 59); n = 0;
          if (c.size - c.pos < size_t(width)) return false;
          for (int i = 0; i < width; ++i) n |= uint64_t(c.data[c.pos++]) << (8 * i);
          ++n;
        }
        if (n > length - dst.size() || n > c.size - c.pos) return false;
        dst.append(reinterpret_cast<const char *>(c.data + c.pos), size_t(n)); c.pos += size_t(n);
      } else {
        uint32_t offset = 0; uint64_t n;
        if (kind == 1) { uint8_t b; if (!c.byte(b)) return false;
          n = 4 + ((tag >> 2) & 7); offset = uint32_t((tag & 0xE0) << 3) | b;
        } else if (kind == 2) { if (c.size - c.pos < 2) return false;
          n = 1 + (tag >> 2); offset = c.data[c.pos] | (uint32_t(c.data[c.pos+1]) << 8); c.pos += 2;
        } else { if (c.size - c.pos < 4) return false;
          n = 1 + (tag >> 2); offset = u32(c.data + c.pos); c.pos += 4;
        }
        if (!offset || offset > dst.size() || n > length - dst.size()) return false;
        for (uint64_t i = 0; i < n; ++i) dst.push_back(dst[dst.size() - offset]);
      }
    }
    return dst.size() == length && c.pos == c.size;
  }
  bool parse_page(Cursor &c, Page &p) {
    int last = 0, id; uint8_t t; int64_t v;
    do {
      if (!c.field(last, id, t)) return false;
      if (!t) break;
      if (id == 1 && integer(c, t, v)) p.type = int(v);
      else if (id == 2 && integer(c, t, v)) p.uncompressed = uint32_t(v);
      else if (id == 3 && integer(c, t, v)) p.compressed = uint32_t(v);
      else if ((id == 5 || id == 7 || id == 8) && t == 12) {
        int inner_last = 0, inner_id; uint8_t inner_t;
        do {
          if (!c.field(inner_last, inner_id, inner_t)) return false;
          if (!inner_t) break;
          if (inner_id == 1 && integer(c, inner_t, v)) p.values = uint32_t(v);
          else if ((id == 5 && inner_id == 2) || (id == 8 && inner_id == 4)) {
            if (!integer(c, inner_t, v)) return false;
            p.encoding = int(v);
          } else if (id == 5 && inner_id == 3) {
            if (!integer(c, inner_t, v)) return false;
            p.def_encoding = int(v);
          } else if (id == 8 && inner_id == 5) {
            if (!integer(c, inner_t, v)) return false;
            p.def_bytes = uint32_t(v);
          } else if (id == 8 && inner_id == 7 && (inner_t == 1 || inner_t == 2)) {
            p.is_compressed = (inner_t == 1);
          } else if (id == 7 && inner_id == 2) {
            if (!integer(c, inner_t, v)) return false;
            p.encoding = int(v);
          } else if (!skip(c, inner_t)) return false;
        } while (true);
      } else if (!skip(c, t)) return false;
    } while (true);
    return p.type >= 0 && p.compressed <= MAX_PAGE && p.uncompressed <= MAX_PAGE;
  }
  static bool hybrid(Cursor &c, uint32_t count, uint8_t bit_width,
                     std::vector<uint32_t> &result) {
    result.clear(); result.reserve(count);
    if (bit_width > 32) return false;
    while (result.size() < count) {
      uint64_t header; if (!c.var(header) || header == 0) return false;
      const uint64_t n = header >> 1;
      if (n > count - result.size() + 7) return false;
      if ((header & 1) == 0) {
        uint32_t value = 0;
        const size_t bytes = (bit_width + 7) / 8;
        if (bytes > c.size - c.pos) return false;
        for (size_t i = 0; i < bytes; ++i) value |= uint32_t(c.data[c.pos++]) << (8 * i);
        for (uint64_t i = 0; i < n; ++i) result.push_back(value);
      } else {
        const uint64_t bytes = n * bit_width;
        if (bytes > c.size - c.pos) return false;
        for (uint64_t i = 0; i < n * 8; ++i) {
          uint32_t value = 0;
          for (uint8_t bit = 0; bit < bit_width; ++bit) {
            const uint64_t bit_index = i * bit_width + bit;
            value |= uint32_t((c.data[c.pos + size_t(bit_index / 8)] >> (bit_index % 8)) & 1) << bit;
          }
          if (result.size() < count) result.push_back(value);
        }
        c.pos += size_t(bytes);
      }
    }
    return true;
  }
  static bool levels(Cursor &c, uint32_t count, std::vector<bool> &present) {
    std::vector<uint32_t> raw;
    if (!hybrid(c, count, 1, raw)) return false;
    present.clear(); present.reserve(count);
    for (uint32_t value : raw) {
      if (value > 1) return false;
      present.push_back(value == 1);
    }
    return true;
  }
  static std::string date_text(int64_t days) {
    int64_t z = days + 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const uint32_t doe = uint32_t(z - era * 146097);
    const uint32_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int64_t year = int64_t(yoe) + era * 400;
    const uint32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const uint32_t mp = (5 * doy + 2) / 153;
    const uint32_t day = doy - (153 * mp + 2) / 5 + 1;
    const uint32_t month = mp < 10 ? mp + 3 : mp - 9;
    year += month <= 2;
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04lld-%02u-%02u",
                  static_cast<long long>(year), month, day);
    return buf;
  }
  static std::string timestamp_text(int64_t raw, int logical) {
    const int64_t unit = logical == 9 ? 1000 : logical == 11 ? 1000000000 : 1000000;
    int64_t seconds = raw / unit;
    int64_t fraction = raw % unit;
    if (fraction < 0) { fraction += unit; --seconds; }
    int64_t days = seconds / 86400;
    int64_t rem = seconds % 86400;
    if (rem < 0) { rem += 86400; --days; }
    char buf[48];
    std::snprintf(buf, sizeof(buf), "%s %02lld:%02lld:%02lld.%06lld",
                  date_text(days).c_str(), static_cast<long long>(rem / 3600),
                  static_cast<long long>((rem / 60) % 60),
                  static_cast<long long>(rem % 60),
                  static_cast<long long>(logical == 9 ? fraction * 1000
                                          : logical == 11 ? fraction / 1000 : fraction));
    return buf;
  }
  static bool plain_cell(Cursor &c, int physical, int logical, Cell &cell) {
    if (physical == 1 || physical == 4) {
      if (c.size - c.pos < 4) return false;
      if (physical == 1) {
        const int32_t value = static_cast<int32_t>(u32(c.data + c.pos));
        cell.text = logical == 6 ? date_text(value) : std::to_string(value);
      }
      else { uint32_t bits = u32(c.data + c.pos); float value;
        std::memcpy(&value, &bits, 4); cell.text = std::to_string(value); }
      c.pos += 4;
    } else if (physical == 2 || physical == 5) {
      if (c.size - c.pos < 8) return false;
      if (physical == 2) {
        const int64_t value = static_cast<int64_t>(u64(c.data + c.pos));
        cell.text = logical == 9 || logical == 10 || logical == 11
            ? timestamp_text(value, logical) : std::to_string(value);
      }
      else { uint64_t bits = u64(c.data + c.pos); double value;
        std::memcpy(&value, &bits, 8); cell.text = std::to_string(value); }
      c.pos += 8;
    } else if (physical == 6) {
      if (c.size - c.pos < 4) return false;
      const uint32_t len = u32(c.data + c.pos); c.pos += 4;
      if (len > c.size - c.pos) return false;
      cell.text.assign(reinterpret_cast<const char *>(c.data + c.pos), len); c.pos += len;
    } else return false;
    return true;
  }
  bool decode_page(const std::string &plain, const Page &page, size_t column,
                   const std::vector<Cell> &dictionary, std::vector<Cell> &out) {
    Cursor c(plain); std::vector<bool> present;
    if (columns_[column].nullable) {
      uint32_t level_bytes = page.def_bytes;
      if (page.type == 0) { if (c.size - c.pos < 4) return false;
        level_bytes = u32(c.data + c.pos); c.pos += 4; }
      if (level_bytes > c.size - c.pos) return false;
      Cursor level(c.data + c.pos, level_bytes);
      if (!levels(level, page.values, present)) return false;
      c.pos += level_bytes;
    } else present.assign(page.values, true);
    const bool encoded_dict = page.encoding == 2 || page.encoding == 8;
    std::vector<uint32_t> indexes;
    if (encoded_dict) {
      uint8_t bit_width;
      uint32_t nonnull = 0;
      for (bool value : present) if (value) ++nonnull;
      if (dictionary.empty() || !c.byte(bit_width)
          || !hybrid(c, nonnull, bit_width, indexes)) return false;
    } else if (page.encoding != 0) return false;
    size_t value_index = 0;
    for (bool exists : present) {
      Cell cell; cell.is_null = !exists;
      if (exists) {
        const int physical = physical_[column];
        if (encoded_dict) {
          if (value_index >= indexes.size() || indexes[value_index] >= dictionary.size()) return false;
          cell = dictionary[indexes[value_index++]];
        } else if (physical == 0) {
          if (value_index / 8 >= c.size - c.pos) return false;
          cell.text = (c.data[c.pos + value_index / 8] >> (value_index % 8)) & 1 ? "1" : "0";
          ++value_index;
        } else if (!plain_cell(c, physical, logical_[column], cell)) return false;
      }
      out.push_back(std::move(cell));
    }
    return true;
  }
  bool open_group(const Group &group) {
    streams_.clear();
    for (size_t i = 0; i < group.chunks.size(); ++i) {
      const Chunk &chunk = group.chunks[i];
      if (projected_[i]
          && (chunk.physical != physical_[i] || chunk.offset < 4 || chunk.offset >= file_size_))
        return fail("unsupported Parquet column layout");
      ColumnStream stream;
      stream.chunk = chunk;
      stream.offset = chunk.dictionary_offset ? chunk.dictionary_offset : chunk.offset;
      streams_.push_back(std::move(stream));
    }
    return true;
  }
  bool load_page(size_t i) {
    if (!unchanged()) return false;
    ColumnStream &stream = streams_[i];
    stream.cells.clear(); stream.cell_index = 0;
    const uint64_t group_rows = groups_[group_index_].rows;
    for (int skipped = 0; skipped < 4; ++skipped) {
        if (stream.offset >= file_size_) return fail("truncated Parquet page");
        std::string header(4096, '\0');
        in_.clear(); in_.seekg(static_cast<std::streamoff>(stream.offset));
        const size_t available = static_cast<size_t>(std::min<uint64_t>(header.size(), file_size_ - stream.offset));
        if (!in_.read(&header[0], available)) return fail("cannot read Parquet page header");
        Cursor c(reinterpret_cast<const uint8_t *>(header.data()), available); Page page;
        if (!parse_page(c, page) || stream.offset + c.pos + page.compressed > file_size_)
          return fail("invalid Parquet page header");
        stream.offset += c.pos;
        std::string encoded(page.compressed, '\0');
        in_.clear(); in_.seekg(static_cast<std::streamoff>(stream.offset));
        if (!in_.read(&encoded[0], page.compressed)) return fail("cannot read Parquet page");
        stream.offset += page.compressed;
        if (page.type != 0 && page.type != 2 && page.type != 3)
          return fail("unsupported Parquet page type");
        std::string plain;
        if (stream.chunk.codec == 0) plain = std::move(encoded);
        else if (stream.chunk.codec == 1) {
          if (page.type == 3) {
            if (page.def_bytes > encoded.size() || page.def_bytes > page.uncompressed)
              return fail("invalid Parquet level length");
            std::string decoded;
            if (page.is_compressed) {
              if (!snappy(encoded.substr(page.def_bytes), page.uncompressed - page.def_bytes, decoded))
                return fail("invalid Snappy page");
            } else {
              decoded = encoded.substr(page.def_bytes);
              if (decoded.size() != page.uncompressed - page.def_bytes)
                return fail("invalid Parquet page length");
            }
            plain = encoded.substr(0, page.def_bytes) + decoded;
          } else if (!snappy(encoded, page.uncompressed, plain)) {
            return fail("invalid Snappy page");
          }
        } else return fail("unsupported Parquet compression");
        if (page.type == 2) {
          if (page.encoding != 0 || page.values > 1000000)
            return fail("unsupported Parquet dictionary");
          Cursor entries(plain);
          for (uint32_t item = 0; item < page.values; ++item) {
            Cell cell;
            if (!plain_cell(entries, physical_[i], logical_[i], cell))
              return fail("invalid Parquet dictionary");
            stream.dictionary.push_back(std::move(cell));
          }
        } else if (!decode_page(plain, page, i, stream.dictionary, stream.cells)) {
          return fail("unsupported Parquet encoding");
        }
        if (stream.cells.size() > group_rows - stream.read)
          return fail("Parquet row count mismatch");
        if (!stream.cells.empty()) return true;
    }
    return fail("empty Parquet pages");
  }
  bool fail(const char *message) { error_ = message; return false; }
  bool unchanged() {
    FileStat stat_buf;
    return file_stat(path_, stat_buf) == 0 && stat_buf.st_size >= 0
        && uint64_t(stat_buf.st_size) == file_size_ && stat_buf.st_mtime == file_mtime_
        ? true : fail("Parquet file changed during scan");
  }
  std::ifstream in_;
  std::string path_, error_;
  uint64_t file_size_ = 0;
  int64_t file_mtime_ = 0;
  std::vector<Column> columns_;
  std::vector<bool> projected_;
  std::vector<int> physical_;
  std::vector<int> logical_;
  std::vector<Group> groups_;
  std::vector<ColumnStream> streams_;
  size_t group_index_ = 0;
  uint64_t row_index_ = 0;
};

}} // namespace oceanbase::sql
