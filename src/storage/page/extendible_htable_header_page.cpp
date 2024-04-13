//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"
#include <cstring>
namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  max_depth_ =max_depth;
  std::memset(directory_page_ids_,INVALID_PAGE_ID,sizeof(directory_page_ids_));
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t
{

  if (max_depth_ == 0) {
      return 0;
  }
  // return hash&((1<<max_depth_)-1);
  return hash >> (sizeof(hash)*8 - max_depth_);
  //利用位操作技巧将high depth位提取出来
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t
{
  // 确保目录索引不超出数组范围
  BUSTUB_ASSERT(directory_idx < HTABLE_HEADER_ARRAY_SIZE, "out of bounds");
   // 返回目录页ID数组中指定索引处的目录页ID
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) 
{
  BUSTUB_ASSERT(directory_idx < HTABLE_HEADER_ARRAY_SIZE, "out of bounds");
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t
{
  return HTABLE_HEADER_ARRAY_SIZE;
}
// void ExtendibleHTableHeaderPage::PrintHeader() const {
//     size_t directory_page_count = sizeof(directory_page_ids_);
//     // size_t directory_page_count = sizeof(directory_page_ids_)/sizeof(page_id_t);
//     // size_t free_size = BUSTUB_PAGE_SIZE-sizeof(directory_page_ids_)/sizeof(page_id_t)-sizeof(max_depth_);//BUSTUB_PAGE_SIZE keyiyong ma??
//     size_t free_size = BUSTUB_PAGE_SIZE-sizeof(directory_page_ids_)-sizeof(max_depth_);//BUSTUB_PAGE_SIZE keyiyong ma??
//     // 输出头部信息
//     std::cout<<" ---------------------------------------------------" << std::endl;
//     std::cout<<"| DirectoryPageIds(" << directory_page_count << ") | MaxDepth (" << max_depth_<< ") | Free(" << free_size << ") |" << std::endl;
//     std::cout<<" ---------------------------------------------------" << std::endl;
// }
}  // namespace bustub
