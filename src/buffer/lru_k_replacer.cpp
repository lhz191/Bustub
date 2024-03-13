//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k)
{

}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool 
{ 
    latch_.lock();
    if(this->curr_size_==0)
    {
        latch_.unlock();
        return false;
    }
    frame_id_t kid=node_store_.begin()->first;
    auto kmax=node_store_.begin()->second;
    for(auto it=node_store_.begin();it!=node_store_.end();it++)
    {
        if(it->second.is_evictable_==true)
        {
            if(it->second>kmax)
            {
                kmax=it->second;
                kid=it->first;
            }
        }
    }
    // std::cout<<kid<<std::endl;
    // std::cout<<"是否可改:"<<node_store_[kid].is_evictable_<<std::endl;
    // std::cout<<"k_now:"<<node_store_[kid].history_.size()<<std::endl;
    if(node_store_[kid].is_evictable_==true)
    {
        *frame_id=kid;
        this->curr_size_--;
        node_store_[kid].history_.clear();
        this->node_store_.erase(kid);
        latch_.unlock();
        return true;
    }
    else
    {
        latch_.unlock();
        return false;
    }

}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) 
{
    latch_.lock();
    size_t frame_size = static_cast<size_t>(frame_id);
    if(frame_size>this->replacer_size_)
    {
        latch_.unlock();
         throw std::invalid_argument("Invalid frame id");
    }
    auto it=node_store_.find(frame_id);
    if(it==node_store_.end())
    {
        LRUKNode l1;
        node_store_[frame_id] = l1;
        node_store_[frame_id] .k_=this->k_;
        it=node_store_.find(frame_id);
    }
    it->second.history_.push_front(current_timestamp_++);
    this->latch_.unlock();
    return;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable)
{
    latch_.lock();
    size_t frame_size = static_cast<size_t>(frame_id);
    if(frame_size>this->replacer_size_)
    {
        latch_.unlock();
         throw std::invalid_argument("Invalid frame id");
    }
    this->node_store_[frame_id].is_evictable_=set_evictable;
    size_t num=0;
    for(auto it=node_store_.begin();it!=node_store_.end();it++)
    {
        if(it->second.is_evictable_==true)
        {
            num++;
        }
    }
    this->curr_size_=num;
    // this->replacer_size_=num;
    latch_.unlock();
    return;
}

void LRUKReplacer::Remove(frame_id_t frame_id)
{
    latch_.lock();
    size_t frame_size = static_cast<size_t>(frame_id);
    if(frame_size>this->replacer_size_)
    {
        latch_.unlock();
         throw std::invalid_argument("Invalid frame id");
    }
    if(this->node_store_[frame_id].is_evictable_==false)
    {
        latch_.unlock();
        throw std::invalid_argument("unevictable id");
    }
    else
    {
        node_store_[frame_id].history_.clear();
        node_store_.erase(frame_id);
        this->curr_size_--;
        latch_.unlock();
        return;
    }
}

auto LRUKReplacer::Size() -> size_t { return this->curr_size_; }

}  // namespace bustub
