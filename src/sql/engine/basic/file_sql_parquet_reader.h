/*
 * Copyright (c) 2026 OceanBase.
 * Licensed under the Apache License, Version 2.0.
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <string>
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
  enum class Type { BOOLEAN, BIGINT, DOUBLE, VARCHAR };
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
    return rescan();
  }

  bool rescan()
  {
    if (!in_.is_open()) return fail("Parquet file is closed");
    group_index_ = 0;
    row_index_ = 0;
    values_.clear();
    return true;
  }

  void close()
  {
    in_.close();
    columns_.clear();
    physical_.clear();
    groups_.clear();
    values_.clear();
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
      values_.clear();
    }
    if (group_index_ >= groups_.size()) return Status::END;
    if (values_.empty() && !load_group(groups_[group_index_])) return Status::ERROR;
    for (const auto &column : values_) {
      if (row_index_ >= column.size()) { fail("short Parquet column"); return Status::ERROR; }
      row.push_back(column[row_index_].text);
      nulls.push_back(column[row_index_].is_null);
    }
    ++row_index_;
    return Status::ROW;
  }

  const std::vector<Column> &columns() const { return columns_; }
  const std::string &error() const { return error_; }

private:
  static constexpr uint64_t MAX_FILE_SIZE = 1ULL << 32;
  static constexpr uint32_t MAX_METADATA = 16U << 20;
  static constexpr uint32_t MAX_PAGE = 64U << 20;
  static constexpr uint32_t MAX_COLUMNS = 1024;
  struct Cell { std::string text; bool is_null = false; };
  struct Chunk { int physical = -1; int codec = -1; uint64_t offset = 0;
                 uint64_t dictionary_offset = 0; uint64_t count = 0; };
  struct Group { uint64_t rows = 0; std::vector<Chunk> chunks; };
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

  bool parse_schema_element(Cursor &c, Column &out, int &children, int &physical) {
    int last = 0, id; uint8_t t; int64_t v; children = 0; physical = -1;
    out = Column{"", Type::VARCHAR, false};
    do {
      if (!c.field(last, id, t)) return false;
      if (t == 0) break;
      if (id == 1 && integer(c, t, v)) physical = int(v);
      else if (id == 3 && integer(c, t, v)) out.nullable = (v == 1);
      else if (id == 4 && string(c, t, out.name)) {}
      else if (id == 5 && integer(c, t, v)) children = int(v);
      else if (!skip(c, t)) return false;
    } while (true);
    if (physical == 0) out.type = Type::BOOLEAN;
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
          Column column; int children, physical;
          if (!parse_schema_element(c, column, children, physical)) return false;
          if (i == 0) { if (children != int(n - 1)) return false; }
          else { if (children != 0 || physical < 0) return false;
            columns_.push_back(column); physical_.push_back(physical); }
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
  static bool plain_cell(Cursor &c, int physical, Cell &cell) {
    if (physical == 1 || physical == 4) {
      if (c.size - c.pos < 4) return false;
      if (physical == 1) cell.text = std::to_string(static_cast<int32_t>(u32(c.data + c.pos)));
      else { uint32_t bits = u32(c.data + c.pos); float value;
        std::memcpy(&value, &bits, 4); cell.text = std::to_string(value); }
      c.pos += 4;
    } else if (physical == 2 || physical == 5) {
      if (c.size - c.pos < 8) return false;
      if (physical == 2) cell.text = std::to_string(static_cast<int64_t>(u64(c.data + c.pos)));
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
        } else if (!plain_cell(c, physical, cell)) return false;
      }
      out.push_back(std::move(cell));
    }
    return true;
  }
  bool load_group(const Group &group) {
    values_.assign(columns_.size(), {});
    for (size_t i = 0; i < group.chunks.size(); ++i) {
      const Chunk &chunk = group.chunks[i];
      if (chunk.physical != physical_[i] || chunk.offset < 4 || chunk.offset >= file_size_)
        return fail("unsupported Parquet column layout");
      uint64_t offset = chunk.dictionary_offset ? chunk.dictionary_offset : chunk.offset;
      std::vector<Cell> dictionary;
      while (values_[i].size() < group.rows) {
        if (offset + 64 > file_size_) return fail("truncated Parquet page");
        std::string header(4096, '\0');
        in_.clear(); in_.seekg(static_cast<std::streamoff>(offset));
        const size_t available = static_cast<size_t>(std::min<uint64_t>(header.size(), file_size_ - offset));
        if (!in_.read(&header[0], available)) return fail("cannot read Parquet page header");
        Cursor c(reinterpret_cast<const uint8_t *>(header.data()), available); Page page;
        if (!parse_page(c, page) || offset + c.pos + page.compressed > file_size_)
          return fail("invalid Parquet page header");
        offset += c.pos;
        std::string encoded(page.compressed, '\0');
        in_.clear(); in_.seekg(static_cast<std::streamoff>(offset));
        if (!in_.read(&encoded[0], page.compressed)) return fail("cannot read Parquet page");
        offset += page.compressed;
        if (page.type != 0 && page.type != 2 && page.type != 3)
          return fail("unsupported Parquet page type");
        std::string plain;
        if (chunk.codec == 0) plain = std::move(encoded);
        else if (chunk.codec == 1) {
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
            if (!plain_cell(entries, physical_[i], cell))
              return fail("invalid Parquet dictionary");
            dictionary.push_back(std::move(cell));
          }
        } else if (!decode_page(plain, page, i, dictionary, values_[i])) {
          return fail("unsupported Parquet encoding");
        }
        if (values_[i].size() > group.rows) return fail("Parquet row count mismatch");
      }
    }
    return true;
  }
  bool fail(const char *message) { error_ = message; return false; }
  std::ifstream in_;
  std::string path_, error_;
  uint64_t file_size_ = 0;
  std::vector<Column> columns_;
  std::vector<int> physical_;
  std::vector<Group> groups_;
  std::vector<std::vector<Cell>> values_;
  size_t group_index_ = 0;
  uint64_t row_index_ = 0;
};

}} // namespace oceanbase::sql
