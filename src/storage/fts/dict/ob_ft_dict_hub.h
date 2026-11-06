/*
 * Copyright (c) 2025 OceanBase.
 *
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

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_HUB_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_HUB_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/lock/ob_bucket_lock.h"
#include "storage/fts/dict/ob_ft_cache_container.h"
#include "storage/fts/dict/ob_ft_dict_def.h"

namespace oceanbase
{
namespace storage
{
// typedef uint64_t ObFTTableID;
// typedef ObFTTableID ObFTDictTableID;

class ObFTDictInfo
{
public:
  ObFTDictInfo()
      : name_(""),
        type_(ObFTDictType::DICT_TYPE_INVALID),
        charset_(CHARSET_INVALID),
        version_(0),
        range_count_(0)
  {
  }

public:
  char name_[2048]; // for now
  ObFTDictType type_;
  ObCharsetType charset_;
  int64_t version_; // in memory
  int32_t range_count_;
};

struct ObFTDictInfoKey
{
public:
  ObFTDictInfoKey()
      : type_(static_cast<uint64_t>(ObFTDictType::DICT_TYPE_INVALID))
  {
  } // default constructor
  ObFTDictInfoKey(const uint64_t type)
      : type_(type)
  {
  }
  int hash(uint64_t &hash_value) const
  {
    int ret = OB_SUCCESS;
    hash_value = hash();
    return ret;
  }

  uint64_t hash() const
  {
    uint64_t hash = 0;
    hash = common::murmurhash(&type_, sizeof(int64_t), hash);
    return hash;
  }

  bool operator==(const ObFTDictInfoKey &other) const
  {
    return type_ == other.type_;
  }

  int compare(const ObFTDictInfoKey &other) const
  {
    int ret = 0;
    if (0 == ret) {
      ret = type_ - other.type_;
    }
    return ret;
  }

private:
  uint64_t type_;
  // name
};

class ObFTCacheRangeContainer;
class ObFTRangeDict;
class ObFTDictHub
{
public:
  ObFTDictHub()
      : is_inited_(false), dict_map_(), rw_dict_lock_(), shared_dict_lock_(), shared_dict_map_(),
        shared_dict_alloc_()
  {
  }
  ~ObFTDictHub() {}

  int init();

  int destroy();

  int build_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container);

  int load_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container);

  /**
   * Return the dictionary of one dictionary type, building it once.
   *
   * A dictionary is read-only once it is built, so one build can serve every
   * parser.  Building the dictionaries again for every segment() call made
   * every indexed row pay for the dictionary setup: one cache lookup per
   * dictionary range and one allocation per range, again and again.
   *
   * The returned dictionary stays owned by the hub, so it outlives the parser
   * that borrowed it and the cache blocks it pins stay valid.
   */
  int get_shared_dict(const ObFTDictDesc &desc, ObFTRangeDict *&dict);

  /**
   * Drop the shared dictionaries so that the next user builds them again.
   */
  void clear_shared_dicts();

private:
  int get_dict_info(const ObFTDictInfoKey &key, ObFTDictInfo &info);

  int put_dict_info(const ObFTDictInfoKey &key, const ObFTDictInfo &info);

private:
  struct ObFTSharedDict
  {
    ObFTCacheRangeContainer container;
    ObFTRangeDict *dict;
    explicit ObFTSharedDict(ObIAllocator &alloc) : container(alloc), dict(nullptr) {}
  };

  int get_shared_dict_info(const ObFTDictInfoKey &key, ObFTSharedDict *&info);

  int put_shared_dict_info(const ObFTDictInfoKey &key, ObFTSharedDict *info);

  int build_shared_dict(const ObFTDictDesc &desc, const ObFTDictInfoKey &key, ObFTSharedDict *&info);

private:
  bool is_inited_;
  // holds info of dict
  hash::ObHashMap<ObFTDictInfoKey, ObFTDictInfo> dict_map_;
  ObBucketLock rw_dict_lock_;
  // The dictionaries that every parser shares.  They have their own lock so
  // that building one does not block the dictionary info above.
  ObBucketLock shared_dict_lock_;
  hash::ObHashMap<ObFTDictInfoKey, ObFTSharedDict *> shared_dict_map_;
  ObArenaAllocator shared_dict_alloc_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_HUB_H_
