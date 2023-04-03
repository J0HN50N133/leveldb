// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
//    kTypeFence varstring varint32
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/version_edit.h"
#include "db/write_batch_internal.h"
#include "db/murmurhash3.h"
#include "leveldb/db.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() { Clear(); }

WriteBatch::~WriteBatch() = default;

WriteBatch::Handler::~Handler() = default;

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      case kTypeFence: {
        uint32_t level;
        if(GetLengthPrefixedSlice(&input, &key) && GetVarint32(&input, &level)) {
          handler->HandleFence(key, level);
        }else {
          return Status::Corruption("unknown WriteBatch tag"); 
        }
      }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}


void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::PutFence(const Slice& key, int level){
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeFence));
  PutLengthPrefixedSlice(&rep_, key);
  PutVarint32(&rep_, level);
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

void WriteBatch::Append(const WriteBatch& source) {
  WriteBatchInternal::Append(this, &source);
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;

  void Put(const Slice& key, const Slice& value) override {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  void Delete(const Slice& key) override {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
  void HandleFence(const Slice& key, unsigned level) override{
    assert(level < config::kNumLevels);
    FenceMetaData* f = new FenceMetaData();
    // TODO: Determine the type
    InternalKey ikey(key, sequence_, kTypeValue);
    f->fence_key = ikey;
    f->level = level;
    f->number_segments = 0;
    f->refs = 1;
    sequence_++;
  }
};

/* 
  class to iterate over WriteBatch and check if a key should be
  a fence. If so, do two things: 
  - add to the string representation (this will later be appended to the
  WriteBatch contents.
  - insert it into the Versions structure. 
*/
 class FenceInserter : public WriteBatch::Handler {
 public:
   FenceInserter() : sequence_(), bit_mask(0) {
     new_batch = NULL;
     for (int i = 0; i < config::kNumLevels; i++)
       num_guards[i] = 0;
   }
   WriteBatch* new_batch;
   SequenceNumber sequence_;

  virtual void Put(const Slice& key, const Slice& value) {
    // Need to hash and check the last few bits. 
    void* input = (void*) key.data();
    size_t size = key.size();
    const unsigned int murmur_seed = 42;
    unsigned int hash_result;
    MurmurHash3_x86_32(input, size, murmur_seed, &hash_result);

    // Go through each level, starting from the top and checking if it
    // is a guard on that level. 
    unsigned num_bits = top_level_bits;
    
    for (unsigned i = 0; i < config::kNumLevels; i++) {
      set_mask(num_bits);
      if ((hash_result & bit_mask) == bit_mask) {
		// found a guard
		// Insert the guard to this level and all the lower levels
		for (unsigned j = i; j < config::kNumLevels; j++) {
			new_batch->PutFence(key, j);
			num_guards[j]++;
		}
		break;
      }
      // Check next level
      num_bits -= bit_decrement;
    }
    sequence_++;
  }

  virtual void Delete(const Slice& key) {
    sequence_++;
  }

  virtual void HandleFence(const Slice& key, unsigned level) {
    /* emptyHandleGuard. */
    assert(0);
  }
  virtual ~FenceInserter(){}
   
  void print_all_levels() {
    for (int i = 0; i < config::kNumLevels; i++)
      if (num_guards[i] > 0)
	printf("%d %d\n", i, num_guards[i]);
  }
   
 private:
   // Number of random bits to match in hash value for key to become
   // top level guard. Note that this has the least probability, will
   // increase as the levels become deeper.
   const static unsigned top_level_bits = 27;
   // Top level guard = 17 bits should match in guard
   // Next level guard = 15 bits
   // Next level guard = 13 bits and so on..
   const static int bit_decrement = 2;

   int num_guards[config::kNumLevels];
   
   unsigned bit_mask;
   
   void set_mask(unsigned num_bits) {
     assert(num_bits > 0 && num_bits < 32);
     bit_mask = (1 << num_bits) - 1;
   }
   
  FenceInserter(const FenceInserter&);
  FenceInserter& operator = (const FenceInserter&);
};
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

Status WriteBatchInternal::SetFences(const WriteBatch* batch, WriteBatch* new_batch){
  FenceInserter f_inserter;
  f_inserter.sequence_ = WriteBatchInternal::Sequence(batch);
  f_inserter.new_batch = new_batch;
  Status s = batch->Iterate(&f_inserter);
  return s;
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
