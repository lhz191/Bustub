//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  // std::cout<<"yes"<<std::endl;
  this->max_size_=max_size;
  // std::cout<<"maxsize"<<max_size_<<std::endl;
  for(uint32_t i=0;i<HTableBucketArraySize(sizeof(std::pair<K, V>));i++)
  {
     array_[i] = std::make_pair(K{}, V{});
  }

}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool 
{
  // std::cout<<"!!!!!!!!"<<std::endl;
  for(uint32_t i=0;i<size_;i++)
  {
    if(!cmp(key,array_[i].first))
    {
      value=array_[i].second;
      return true;
    }
  }
  return false;
}
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup1(const K &key, std::vector<V>& value, const KC& cmp) const-> bool {
  // for(uint32_t j = 0; j < size_; j++)
  // {
  //    std::cout<<"array_[j]"<<array_[j].first<<array_[j].second<<std::endl;
  // }
    for (uint32_t i = 0; i < size_; i++) {
        if (!cmp(key, array_[i].first)) {
          // std::cout<<"array_[i]"<<array_[i].first<<array_[i].second<<std::endl;
            value.push_back(array_[i].second);
        }
    }
    if (!value.empty()) {
        return true;
    }
        return false;
}
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  // std::cout<<"this->size_"<<this->size_<<std::endl;
  // std::cout<<"max_size_"<<max_size_<<std::endl;
  // if(this->size_>max_size_)
  // {
  //   // std::cout<<"this->size_"<<this->size_<<std::endl;
  //   // std::cout<<"max_size_"<<max_size_<<std::endl;
  //   std::cout<<"return1"<<std::endl;
  //   return false;
  // }
  // if(Lookup(key,value,cmp)==true)
  // {
  //   return false;
  // }
  if(this->IsFull())
  {
    return false;
  }
  // for(uint32_t i=0;i<size_;i++)
  // {
  //   if(!cmp(key,array_[i].first))
  //   {
  //     return false;
  //   }
  // }
  // std::cout<<"ture"<<std::endl;
  this->array_[size_]=std::make_pair(key, value);
  size_++;
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool 
{
  for(uint32_t i=0;i<size_;i++)
  {
    if(!cmp(key,array_[i].first))
    {
      if(i!=size_-1)
      {
        for(uint32_t j=i;j<size_-1;j++)
        {
          array_[j]=array_[j+1];
        }
      }
    size_--;
    return true;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  for (uint32_t i = bucket_idx; i < size_ - 1; i++) {
    array_[i] = array_[i + 1];
  }
  size_--;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
   if (bucket_idx >= size_) {
    throw std::out_of_range("bucket_idx out of range");
  }
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
   if (bucket_idx >= size_) {
    throw std::out_of_range("bucket_idx out of range");
  }
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
    if (bucket_idx >= size_) {
    throw std::out_of_range("bucket_idx out of range");
  }
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
   return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ == max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
