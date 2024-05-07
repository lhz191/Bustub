//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
      index_name_ = name;
      pages_ = bpm_->GetPages();
    // 初始化头部页面
    auto new_page = bpm_->NewPageGuarded(&header_page_id_);
    auto header_page = new_page.UpgradeWrite();
    auto htable_header = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_page.GetDataMut());
    htable_header->Init(header_max_depth);
    header_page.Drop();
    // // 初始化目录页面
    // page_id_t directory_page_id;
    // auto new_dir_page = bpm_->NewPageGuarded(&directory_page_id);
    // auto dir_page = new_dir_page.UpgradeWrite();
    // auto dir_page_ptr = reinterpret_cast<ExtendibleHTableDirectoryPage *>(dir_page.GetDataMut());
    // dir_page_ptr->Init(directory_max_depth);
    // dir_page.Drop();

    // // 初始化桶页面
    // page_id_t bucket_page_id;
    // auto new_bucket_page = bpm_->NewPageGuarded(&bucket_page_id);
    // auto bucket_page = new_bucket_page.UpgradeWrite();
    // auto bucket_page_ptr = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page.GetDataMut());
    // std::cout<<"bucket_max_size"<<bucket_max_size<<std::endl;
    // bucket_page_ptr->Init(bucket_max_size);
    // bucket_page.Drop();
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const-> bool {
    uint32_t hash =this->Hash(key);
    // std::cout<<"hash"<<hash<<std::endl;
    auto header_page = this->bpm_->FetchPageRead(header_page_id_);
    // std::cout<<"header_page_id_:________________"<<header_page_id_<<std::endl;//quan shi 0 zhengming shiduide
    auto header = reinterpret_cast<const ExtendibleHTableHeaderPage*>(header_page.GetData());
    uint32_t dict_idx = header->HashToDirectoryIndex(hash);
    page_id_t dict_id = header->GetDirectoryPageId(dict_idx);
    // std::cout<<"dict_id"<<dict_id<<std::endl;
    if(dict_id==INVALID_PAGE_ID)
    {
        header_page.Drop();
        // std::cout<<"alsotrue1"<<std::endl;
        return false;
    }
    header_page.Drop();  // 不 drop 一直有锁
    auto dict_page= this->bpm_->FetchPageRead(dict_id);
    // std::cout<<"alsotrue1"<<std::endl;
    auto dict = reinterpret_cast<const ExtendibleHTableDirectoryPage*>(dict_page.GetData());
    uint32_t bucket_idx = dict->HashToBucketIndex(hash);
    page_id_t bucket_id = dict->GetBucketPageId(bucket_idx);
    dict_page.Drop();
    if(dict_id==INVALID_PAGE_ID)
    {
        //   std::cout<<"alsotrue2"<<std::endl;
        return false;
    }
    auto bucket_page = this->bpm_->FetchPageRead(bucket_id);
    // std::cout<<"alsotrue1"<<std::endl;
    auto bucket = reinterpret_cast<const ExtendibleHTableBucketPage<K, V, KC>*>(bucket_page.GetData());
    //zhe li ying gai xuyao fan hui suoyou fuhe de V
    //no!!This semester you will only need to support unique key-value pairs.!!
    // V result1;
    // if (bucket->Lookup(key, result1, this->cmp_)) {
    //     result->push_back(result1);
    //     std::cout<<"bucket_page.PageId()"<<bucket_page.PageId()<<std::endl;
    //     bucket_page.Drop();
    //     return true;
    // }
    // std::vector<V> *result1;
        if (bucket->Lookup1(key, *result, this->cmp_)) {
        // result->push_back(result1);
        // std::cout<<"bucket_page.PageId()"<<bucket_page.PageId()<<std::endl;
        bucket_page.Drop();
        return true;
    }
    bucket_page.Drop();
    return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
    // std::cout<<"flag1"<<std::endl;
    uint32_t hash = this->Hash(key);
    // std::cout<<"insert_hash:"<<hash<<std::endl;
    // 读取头部页面
    auto header_page = this->bpm_->FetchPageWrite(header_page_id_);
    // std::cout<<"flag2"<<std::endl;
    // std::cout<<"flag3"<<std::endl;
    auto header = reinterpret_cast<ExtendibleHTableHeaderPage*>(header_page.GetDataMut());
    // header->Init(header_max_depth_);
    // 计算目录页面的索引
    auto dict_idx = header->HashToDirectoryIndex(hash);
    // std::cout<<"dict_idx:"<<dict_idx<<std::endl;
    const int dict_id = header->GetDirectoryPageId(dict_idx);
    // std::cout<<"dict_id:"<<dict_id<<std::endl;
    header_page.Drop();  
    if (dict_id == INVALID_PAGE_ID) {
        // std::cout<<"rhasdsadsad"<<std::endl;
        // 如果目录页面还未创建,则创建一个新的目录页面和桶页面
            // bucket_page.Drop();
            // dict_page.Drop();
        return InsertToNewDirectory(header, dict_idx, hash, key, value);
    }
    // 读取目录页面
    // std::cout<<"flag22"<<std::endl;
    auto dict_page = this->bpm_->FetchPageWrite(dict_id);
    auto dict = reinterpret_cast<ExtendibleHTableDirectoryPage*>(dict_page.GetDataMut());
    // dict->Init(directory_max_depth_);
    dict_page.Drop();
    // 计算桶页面的索引
    uint32_t bucket_idx = dict->HashToBucketIndex(hash);
    // std::cout<<bucket_idx<<"bucket_idx"<<std::endl;
    page_id_t bucket_id = dict->GetBucketPageId(bucket_idx);
    // std::cout<<bucket_id<<"bucket_id"<<std::endl;
    if (bucket_id == INVALID_PAGE_ID) {
        // 如果桶页面还未创建,则创建一个新的桶页面
        return InsertToNewBucket(dict, bucket_idx, key, value);
    }
    // 读取桶页面
    // std::cout<<"flag34"<<std::endl;
    auto bucket_page = this->bpm_->FetchPageWrite(bucket_id); 
    // std::cout<<"flag33"<<std::endl;
    auto bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page.GetDataMut());

    // std::cout<<"bucket->IsFull()"<<bucket->IsFull()<<std::endl;
    if (!bucket->IsFull()) {
        // 如果桶页面未满,则直接插入键值对
        // std::cout<<"yes____________________"<<std::endl;
        bool success = bucket->Insert(key, value, this->cmp_);
        bucket_page.Drop();
        return success;
    }
    
    // 如果桶页面已满,则需要进行拆分
    while (bucket->IsFull()) {
        // std::cout<<"full____________________"<<std::endl;
        uint32_t local_depth = dict->GetLocalDepth(bucket_idx);
        uint32_t global_depth = dict->GetGlobalDepth();
        // std::cout<<global_depth<<std::endl;
        // std::cout<<local_depth<<std::endl;
        // std::cout<<dict->GetMaxDepth()<<std::endl;
        if (global_depth == dict->GetMaxDepth() && local_depth == global_depth) {
            // 全局深度已达到最大,无法再进行目录扩展
            bucket_page.Drop();
            return false;
        }
        
        // 分配一个新的桶页面
        page_id_t new_bucket_page_id;
        auto new_page = bpm_->NewPageGuarded(&new_bucket_page_id);
        auto new_bucket_page = new_page.UpgradeWrite();
        auto new_htable_bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(new_bucket_page.GetDataMut());
        new_htable_bucket->Init(bucket_max_size_);
        // 如果全局深度等于局部深度,则需要扩展目录页面
        if (global_depth == local_depth) {
            // std::cout<<"chaifen!_______"<<std::endl;
            dict->IncrGlobalDepth();
            dict->IncrLocalDepth(bucket_idx);
            uint32_t newidx=(1 << (global_depth)) + bucket_idx;
            dict->bucket_page_ids_[newidx]=new_bucket_page_id;
            // std::cout<<"new_idx"<<newidx<<std::endl;
            // std::cout<<"new_bucket_page_id"<<new_bucket_page_id<<std::endl;
            new_bucket_page.Drop();
            dict->IncrLocalDepth(newidx);
            // std::cout<<"dict->GetLocalDepthMask(bucket_idx)"<<dict->GetLocalDepthMask(bucket_idx)<<std::endl;
            this->MigrateEntries(bucket, new_htable_bucket, newidx, dict->GetLocalDepthMask(bucket_idx));
            // for(int temp_bucket_index=0;temp_bucket_index<(1<<dict->GetGlobalDepth());temp_bucket_index++)
            // {
            //     auto temp_bucket_page = this->bpm_->FetchPageWrite(dict->bucket_page_ids_[temp_bucket_index]);
            //     // std::cout<<"flag33"<<std::endl;
            //     auto temp_bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(temp_bucket_page.GetDataMut());
            //     temp_bucket_page.Drop();
            //     for(int j=temp_bucket_index+1;j<(1<<dict->GetGlobalDepth());j++)
            //     {
            //     auto temp2_bucket_page = this->bpm_->FetchPageWrite(dict->bucket_page_ids_[j]);
            //     // std::cout<<"flag33"<<std::endl;
            //     auto temp2_bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(temp2_bucket_page.GetDataMut());
            //     temp2_bucket_page.Drop();
            //     this->MigrateEntries(temp_bucket,temp2_bucket, j, dict->GetLocalDepthMask(temp_bucket_index));
            //     }
            // }
        } else {
            new_bucket_page.Drop();
            // 否则,只需要在目录页面中添加一个新的桶页面
            dict->SetBucketPageId(bucket_idx + (1 << local_depth), new_bucket_page_id);
            dict->local_depths_[bucket_idx + (1 << local_depth)]=local_depth;
            this->MigrateEntries(bucket, new_htable_bucket, bucket_idx + (1 << local_depth), dict->GetLocalDepthMask(bucket_idx));
            dict->IncrLocalDepth(bucket_idx);
            dict->IncrLocalDepth(bucket_idx + (1 << local_depth));
        }
        // 更新目录页面和桶页面
        // dict_page.Drop();
        // dict_page = this->bpm_->FetchPageWrite(dict_id);
        // dict = reinterpret_cast<ExtendibleHTableDirectoryPage*>(dict_page.GetDataMut());
        // bucket_page.Drop();
        // bucket_page = this->bpm_->FetchPageWrite(bucket_id);
        // bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page.GetDataMut());
    }
    bool success = bucket->Insert(key, value, this->cmp_);
    bucket_page.Drop();
    return success;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool 
{
    page_id_t dict_id;
    BasicPageGuard page_guard=this->bpm_->NewPageGuarded(&dict_id);
    // page_id_t dict_id = -1;
    header->SetDirectoryPageId(directory_idx,dict_id);
    // this->bpm_->NewPage(&dict_id);
    auto dict_page= page_guard.UpgradeWrite();
    auto dict=reinterpret_cast<ExtendibleHTableDirectoryPage*>(dict_page.GetDataMut());//zheli Mut()设置dirty
    dict->Init(directory_max_depth_);
    dict_page.Drop();
    uint32_t bucket_idx = dict->HashToBucketIndex(hash);
    // page_id_t bucket_id = this->bpm_->AllocatePage();

    page_id_t bucket_id;
    BasicPageGuard bucket_guard=this->bpm_->NewPageGuarded(&bucket_id );
    dict->SetBucketPageId(bucket_idx,bucket_id);
    // std::cout<<"bucket_id"<<bucket_id<<std::endl;
    auto bucket_page= bucket_guard.UpgradeWrite();
    auto bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page.GetDataMut());
    bucket->Init(bucket_max_size_);
    bool success= bucket->Insert(key,value,this->cmp_);
    bucket_page.Drop();
    return success;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool
{
    // page_id_t bucket_id = this->bpm_->AllocatePage();
    page_id_t bucket_id;
    BasicPageGuard bucket_guard=this->bpm_->NewPageGuarded(&bucket_id );
    directory->SetBucketPageId(bucket_idx,bucket_id);
    auto bucket_page= bucket_guard.UpgradeWrite();
    auto bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page.GetDataMut());
    bool success= bucket->Insert(key,value,this->cmp_);
    bucket_page.Drop();
    return success;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t old_bucket_idx) 
{
    directory->SetLocalDepth(old_bucket_idx,0);
    directory->bucket_page_ids_[old_bucket_idx]=new_bucket_page_id;
    directory->SetLocalDepth(new_bucket_idx,new_local_depth);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
    // 计算键的哈希值
    uint32_t hash = this->Hash(key);

    // 从头部页面获取目录页面的索引
    auto header_page = this->bpm_->FetchPageWrite(this->header_page_id_);
    auto header = reinterpret_cast<ExtendibleHTableHeaderPage*>(header_page.GetDataMut());
    uint32_t dict_idx = header->HashToDirectoryIndex(hash);
    page_id_t dict_page_id = header->GetDirectoryPageId(dict_idx);
    header_page.Drop();
    // 如果目录页面ID无效,则说明该键不存在
    if (dict_page_id == INVALID_PAGE_ID) {
        return false;
    }
    // 获取目录页面
    auto dict_page = this->bpm_->FetchPageWrite(dict_page_id);
    auto dict = reinterpret_cast<ExtendibleHTableDirectoryPage*>(dict_page.GetDataMut());
    // 计算桶页面的索引
    uint32_t bucket_idx = dict->HashToBucketIndex(hash);
    page_id_t bucket_id = dict->GetBucketPageId(bucket_idx);
    dict_page.Drop();
    // 如果桶页面ID无效,则说明该键不存在
    if (bucket_id == INVALID_PAGE_ID) {
        return false;
    }

    // 获取桶页面并从中删除键值对
    auto bucket_page = this->bpm_->FetchPageWrite(bucket_id);
    auto bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC>*>(bucket_page.GetDataMut());
    bool removed = bucket->Remove(key, this->cmp_);
    bucket_page.Drop();

    // 如果删除失败,则返回false
    if (!removed) {
        return false;
    }
    // std::cout<<"______flag1________"<<std::endl;
    // 检查是否需要合并桶
    uint32_t local_depth = dict->GetLocalDepth(bucket_idx);
    if (local_depth > 0) {
        uint32_t global_depth_=dict->GetGlobalDepth();
        // std::cout<<"global_depth_"<<global_depth_<<std::endl;

        uint32_t merge_bucket_idx;
        for(uint32_t i=0;i<static_cast<uint32_t>(1<<(global_depth_)); i++){
            // std::cout<<"______flag2________"<<std::endl;
        if(bucket_idx>=static_cast<uint32_t>(1 << (global_depth_-1))&&(i<static_cast<uint32_t>(1<<(global_depth_)))){

            merge_bucket_idx = bucket_idx -(1<<(global_depth_-1));
            // std::cout<<"______________!!!!!!!____________bucket_idx:"<<bucket_idx<<"merge_bucket_idx:"<<merge_bucket_idx<<std::endl;
            uint32_t merge_local_depth = dict->GetLocalDepth(merge_bucket_idx);
            page_id_t merge_bucket_id = dict->GetBucketPageId(merge_bucket_idx);
            if (merge_local_depth == local_depth && bucket->IsEmpty() ) {
            // 将非空桶的页面ID更新到目录页面
            this->UpdateDirectoryMapping(dict, merge_bucket_idx, merge_bucket_id, local_depth - 1, bucket_idx);
            this->bpm_->DeletePage(bucket_id);
        }
        }
        else
        {
            merge_bucket_idx = bucket_idx +(1<<(global_depth_-1));
            // std::cout<<"________!!!!!!!!!!!!!__________________bucket_idx:"<<bucket_idx<<"merge_bucket_idx:"<<merge_bucket_idx<<std::endl;
            uint32_t merge_local_depth = dict->GetLocalDepth(merge_bucket_idx);
            page_id_t merge_bucket_id = dict->GetBucketPageId(merge_bucket_idx);
            if (merge_local_depth == local_depth && bucket->IsEmpty() ) {
            // 将非空桶的页面ID更新到目录页面
            this->UpdateDirectoryMapping(dict, merge_bucket_idx, merge_bucket_id, local_depth - 1, bucket_idx);
            this->bpm_->DeletePage(bucket_id);
        }
        }
        // uint32_t merge_local_depth = dict->GetLocalDepth(merge_bucket_idx);
        // page_id_t merge_bucket_id = dict->GetBucketPageId(merge_bucket_idx);

        // // // 如果对应的分裂桶为空,则合并
        // // auto merge_bucket_page = this->bpm_->FetchPageRead(merge_bucket_id);
        // // auto merge_bucket = reinterpret_cast<const ExtendibleHTableBucketPage<K, V, KC>*>(merge_bucket_page.GetData());
        // if (merge_local_depth == local_depth && bucket->IsEmpty() ) {
        //     // 将非空桶的页面ID更新到目录页面
        //     this->UpdateDirectoryMapping(dict, merge_bucket_idx, merge_bucket_id, local_depth - 1, 0);
        //     this->bpm_->DeletePage(bucket_id);
        //     local_depth--;
        // }
        // merge_bucket_page.Drop();
    }
    }

    // 检查是否可以缩小全局深度
    while (dict->CanShrink()) {
        dict->DecrGlobalDepth();
    }

    return true;
}
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                      ExtendibleHTableBucketPage<K, V, KC> *new_bucket, uint32_t new_bucket_idx,
                      uint32_t local_depth_mask)
{
    // local_depth_mask++;
        // 遍历旧桶中的所有键值对
    // for (auto it = old_bucket->Begin(); it != old_bucket->End(); ++it) {
    //     const K& key = it->first;
    //     const V& value = it->second;
for (uint32_t i = 0; i < old_bucket->Size(); ++i) {
    K key = old_bucket->KeyAt(i);
    uint32_t new_idx = Hash(key);
     if (new_idx == new_bucket_idx) {
        auto &entry = old_bucket->EntryAt(i);
        new_bucket->Insert(entry.first, entry.second, cmp_);
        old_bucket->RemoveAt(i);
    }
}
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
