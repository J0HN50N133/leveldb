// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include "db/dbformat.h"
#include <set>
#include <utility>
#include <vector>

namespace leveldb {

class VersionSet;

struct FenceMetaData;

struct FileMetaData {
  FileMetaData()
      : refs(0),
        allowed_seeks(1 << 30),
        file_size(0),
        smallest(),
        largest(),
        fence() {}

  int refs;
  int allowed_seeks;  // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;    // File size in bytes
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
  FenceMetaData* fence;  // The fence that the file belongs to.
};

/*
fence_key is the smallest key served by the fence file. In each level,
there can be only one fence starting with a given key, so (level, key)
uniquely identifies a fence.
*/
struct FenceMetaData {
  int refs;
  int level;
  uint64_t number_segments;
  InternalKey fence_key;  // fence key is selected before any keys are inserted
  /* Need not be same as fence_key. Ex: g: 100, smallest: 102 */
  InternalKey smallest;
  InternalKey largest;  // Largest internal key served by table
  // The list of file numbers that form a part of this fence.
  std::vector<uint64_t> files;
  std::vector<FileMetaData*> file_metas;

  FenceMetaData()
      : refs(0),
        level(-1),
        fence_key(),
        smallest(),
        largest(),
        number_segments(0) {
    files.clear();
  }
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  void AddFileToSentinel(FileMetaData* f, int level) {
    assert(0 <= level && level < config::kNumLevels);
    auto meta_data = *f;
    sentinel_files_[level].push_back(meta_data);
  }

  void AddSentinelFile(int level, int allowed_seeks, uint64_t file_size,
                       FenceMetaData* f, InternalKey largest,
                       InternalKey smallest, uint64_t number, int refs) {
    FileMetaData meta;
    meta.allowed_seeks = allowed_seeks;
    meta.file_size = file_size;
    meta.fence = f;
    meta.largest = largest;
    meta.smallest = smallest;
    meta.number = number;
    meta.refs = refs;
    sentinel_files_[level].push_back(meta);
  }

  void AddSentinelFileNo(int level, uint64_t no) {
    sentinel_file_nos_[level].push_back(no);
  }

  void AddFence(int level, const InternalKey& fence_key) {
    assert(level >= 0 && level < config::kNumLevels);
    FenceMetaData g;
    g.fence_key = fence_key;
    g.level = level;
    g.number_segments = 0;
    new_fences_[level].push_back(g);
  }

  void AddCompleteFence(int level, const InternalKey& fence_key) {
    assert(level >= 0 && level < config::kNumLevels);
    FenceMetaData f;
    f.fence_key = fence_key;
    f.level = level;
    f.number_segments = 0;
    new_complete_fences_[level].push_back(f);
  }

  void AddFenceFromExisting(int level, FenceMetaData* g) {
    assert(level >= 0 && level < config::kNumLevels);
    FenceMetaData new_g(*g);
    new_fences_[level].push_back(new_g);
  }

  void AddCompleteFenceFromExisting(int level, FenceMetaData* f) {
    assert(level >= 0 && level < config::kNumLevels);
    FenceMetaData new_f(*f);
    new_complete_fences_[level].push_back(new_f);
  }

  /* A version of AddFence that contains files. */
  void AddFenceWithFiles(int level, uint64_t number_segments,
			 const InternalKey& fence_key,
			 const InternalKey& smallest,
			 const InternalKey& largest,
			 const std::vector<uint64_t> files) {
    assert(level >= 0 && level < config::kNumLevels);
    FenceMetaData g;
    g.fence_key = fence_key;
    g.level = level;
    g.smallest = smallest;
    g.largest = largest;
    g.number_segments = number_segments;
    g.files.insert(g.files.end(), files.begin(), files.end());
    new_fences_[level].push_back(g);
  }

  // Delete the specified "file" from the specified "level".
  void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void DeleteFence(int level, InternalKey fence) {
    deleted_fences_.insert(std::make_pair(level, fence));
  }

  void DeleteSentinelFile(int level, uint64_t file) {
    deleted_sentinel_files_.insert(std::make_pair(level, file));
  }
  
  void UpdateFences(uint64_t* fence_array) {
    for (int i = 0; i < config::kNumLevels; i++) {
      fence_array[i] += new_fences_[i].size();
    }
  }
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::pair<int, InternalKey> FencePair;

  struct BySmallestPair {
    const InternalKeyComparator* internal_comparator;
    auto operator()(FencePair p1, FencePair p2) const -> bool {
      if (p1.first != p2.first) {
        return p1.first < p2.first;
      } else {
        int r = internal_comparator->Compare(p1.second, p2.second);
        return r < 0;
      }
    }
  };

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;
  typedef std::set<FencePair, BySmallestPair> DeletedFenceSet;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector<std::pair<int, InternalKey>> compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector<std::pair<int, FileMetaData>> new_files_;

  /* Fence Related */
  std::vector<FileMetaData> sentinel_files_[config::kNumLevels];
  std::vector<uint64_t> sentinel_file_nos_[config::kNumLevels];
  DeletedFileSet deleted_sentinel_files_;

  std::vector<FenceMetaData> new_fences_[config::kNumLevels];
  std::vector<FenceMetaData> new_complete_fences_[config::kNumLevels];
  DeletedFenceSet deleted_fences_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
