//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_=max_depth;
  global_depth_=0;//chushi wei 0
  std::memset(local_depths_,0,sizeof(local_depths_));
  std::memset(bucket_page_ids_,INVALID_PAGE_ID,sizeof(bucket_page_ids_));
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t
{
  return hash&((1<<global_depth_)-1);
}
auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t
{
    if (bucket_idx>=HTABLE_DIRECTORY_ARRAY_SIZE) {
        throw std::invalid_argument("out of bounds");
  }
  // 返回目录页ID数组中指定索引处的目录页ID
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
    if (bucket_idx>=HTABLE_DIRECTORY_ARRAY_SIZE) {
        throw std::invalid_argument("out of bounds");
  }
  auto count = 1 << global_depth_;
  for (auto idx = 0; idx < count; ++idx) {
      if (bucket_page_ids_[idx] == bucket_page_id) {//之前没有指向page的，local_depth就为0
          local_depths_[bucket_idx] = local_depths_[idx];
          break;
      }
      if (idx == count - 1) {
          local_depths_[bucket_idx] = global_depth_;//special !!
      }
  }
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t dict_idx) const -> uint32_t
{
  return (1 << global_depth_) + dict_idx;//zhe li global depth是增加（改变）前的!!
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t
{
  return this->global_depth_;
}
auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t
{
  return this->max_depth_;
}
auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t
{
  uint32_t global_depth_mask =  ((1 << GetGlobalDepth()) - 1);
  return global_depth_mask; 
}
auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t
{
  uint32_t local_depth_mask =  ((1 << GetLocalDepth(bucket_idx)) - 1);
  return local_depth_mask; 
}
void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  // if (global_depth_<0) {
  //       throw std::invalid_argument("wrong");
  // }
  for(uint32_t i=(1<<global_depth_);i<(static_cast<uint32_t>(1)<<(global_depth_+1));i++)
  {
    page_id_t temp=bucket_page_ids_[i-(1<<global_depth_)];
    uint32_t newid=GetSplitImageIndex(i-(1<<global_depth_));
    bucket_page_ids_[newid]=temp;
    local_depths_[newid]=local_depths_[i-(1<<global_depth_)];
  }
  this->global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_<=0) {
    throw std::invalid_argument("wrong");
  }
  if(this->global_depth_>0){
  this->global_depth_--;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool
{
  for(uint32_t i=0;i<((static_cast<uint32_t>(1)<<global_depth_));i++)
  {
    if(local_depths_[i]==global_depth_)
    {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t
{
  //   if (global_depth_<0) {
  //       throw std::invalid_argument("wrong");
  // }
  return 1<<global_depth_;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t
{
  //   if (bucket_idx<0) {
  //       throw std::invalid_argument("wrong");
  // }
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) 
{
  //   if (bucket_idx<0) {
  //       throw std::invalid_argument("wrong");
  // }
  local_depths_[bucket_idx]=local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  //   if (bucket_idx<0) {
  //       throw std::invalid_argument("wrong");
  // }
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  local_depths_[bucket_idx]--;
}

}  // namespace bustub
