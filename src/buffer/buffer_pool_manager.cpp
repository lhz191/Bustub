//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_ + 5020];

  replacer_ = std::make_unique<LRUKReplacer>(pool_size+5000, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_+5000; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  this->latch_.lock();
  if (this->free_list_.empty() == true) {
    int flag = 1;
    auto it = this->page_table_.begin();
    for (; it != this->page_table_.end(); it++) {
      if (this->pages_[it->first].pin_count_ ==0) 
      {
        flag = 0;
        break;
      }
    }
    if (flag == 1) {
      // std::cout<<"returnthis"<<std::endl;
      this->latch_.unlock();
      return nullptr;
    }
  }
  page_id_t newid = this->AllocatePage();
  frame_id_t temp = 0;
  if (this->free_list_.empty() != true) {
    temp = this->free_list_.front();
    free_list_.pop_front();
    this->page_table_.emplace(newid, temp);
    Page &new_page = pages_[newid];
    // new_page=*new Page();
    // Page &new_page=*new Page();
    // new_page=pages_[newid];W
    new_page.is_dirty_ = false;
    new_page.pin_count_ = 1;
    this->replacer_->SetEvictable(temp, false);
    this->replacer_->RecordAccess(temp);
    new_page.page_id_ = newid;
    *page_id = newid;
    // this->disk_scheduler_->disk_manager_ ->WritePage(newid,this->pages_[newid].data_);
    latch_.unlock();
    return &pages_[newid];
  } else {
    if (this->replacer_->Evict(&temp) != 1) {
      this->latch_.unlock();
      // std::cout<<"returnthis2"<<std::endl;
      return nullptr;  // 无法创建新页面
    }
    page_id_t temp_id;
    auto pair = page_table_.begin();
    for (; pair != page_table_.end(); pair++) {
      if (pair->second == temp) {
        temp_id = pair->first;  // fan hui page_id
        break;
      }
    }
    Page *temp_page = nullptr;
    temp_page = &pages_[temp_id];
    if (pages_[temp_id].is_dirty_ == true) {
      this->latch_.unlock();
      this->FlushPage(temp_id);
      this->latch_.lock();
      temp_page->ResetMemory();
    }
    this->replacer_->SetEvictable(temp, false);
    this->replacer_->RecordAccess(temp);
    temp_page->page_id_ = newid;
    // pages_[temp_id].pin_count_++;
    Page &new_page = pages_[newid];
    this->page_table_.erase(temp_id);
    new_page.is_dirty_ = false;
    new_page.pin_count_ = 1;
    this->page_table_.emplace(newid, temp);
    this->replacer_->node_store_.erase(temp);  // important!!!
    *page_id = newid;
    this->disk_scheduler_->disk_manager_->WritePage(newid, this->pages_[newid].data_);
    latch_.unlock();
    return &pages_[newid];
  }
latch_.unlock();
// std::cout<<"returnthis1"<<std::endl;
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  this->latch_.lock();
  frame_id_t frame1 = 0;
  auto it2 = this->page_table_.find(page_id);
  if (it2 == page_table_.end()) {
    if (!this->free_list_.empty()) {
      // NOT null,fetch frame and duqu from disk
      frame1 = this->free_list_.front();
      free_list_.pop_front();
      page_id_t temp_id;
      for (auto &pair : this->page_table_) {
        if (pair.second == frame1) {
          temp_id = pair.first;  // fan hui page_id
        }
      }
      pages_[temp_id].pin_count_++;
      this->replacer_->SetEvictable(frame1, false);
      this->replacer_->RecordAccess(frame1);
      this->disk_scheduler_->disk_manager_->ReadPage(temp_id, this->pages_[temp_id].data_);
      page_id = temp_id;
      this->latch_.unlock();
      return &pages_[temp_id];
    } else {
      frame_id_t temp = 0;
      if (this->replacer_->Evict(&temp) != 1) {
        this->latch_.unlock();
        return nullptr;  // 无法fetch页面
      }
      page_id_t temp_id;
      for (auto &pair : this->page_table_) {
        if (pair.second == temp) {
          temp_id = pair.first;  // fan hui page_id
        }
        Page *temp_page = nullptr;
        temp_page = &pages_[temp_id];
        if (this->pages_[temp_id].is_dirty_ == true) {
          this->latch_.unlock();
          this->FlushPage(temp_id);
          this->latch_.lock();
          temp_page->ResetMemory();
        }
        this->disk_scheduler_->disk_manager_->ReadPage(temp_id, this->pages_[temp_id].data_);
        this->replacer_->SetEvictable(temp, false);
        this->replacer_->RecordAccess(temp);
        this->replacer_->node_store_.erase(temp);
        pages_[temp_id].pin_count_++;
        page_id = temp_id;
        latch_.unlock();
        return &pages_[temp_id];
      }
    }
  } else {
    // find!!!
    page_id = it2->first;
    this->replacer_->SetEvictable(it2->second, false);
    this->replacer_->RecordAccess(it2->second);
    pages_[page_id].pin_count_++;
    latch_.unlock();
    return &pages_[page_id];
  }
  latch_.unlock();
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  // std::cout<<"unpin"<<std::endl;
  auto it = this->page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return false;
  } else if (pages_[page_id].pin_count_ == 0) {
    frame_id_t frame1 = this->page_table_[page_id];
    this->replacer_->node_store_[frame1].is_evictable_ = true;
    latch_.unlock();
    return false;
  } else {
    pages_[page_id].pin_count_ = 0;
    frame_id_t frame1 = this->page_table_[page_id];

    this->replacer_->curr_size_++;
    this->replacer_->RecordAccess(frame1);
    this->replacer_->node_store_[frame1].is_evictable_ = true;
    pages_[page_id].is_dirty_ = is_dirty;
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  this->latch_.lock();
  if (page_id == INVALID_PAGE_ID) {
    this->latch_.unlock();
    return false;
  }
  // std::unordered_map<page_id_t, frame_id_t> page_table_;用于跟踪缓冲池页面的页表
  auto it = this->page_table_.find(page_id);
  if (it == this->page_table_.end()) {
    latch_.unlock();
    return false;
  } else {
    this->disk_scheduler_->disk_manager_->WritePage(page_id, this->pages_[page_id].data_);
    this->pages_[page_id].is_dirty_ = false;
    latch_.unlock();
    return true;
  }
}

void BufferPoolManager::FlushAllPages() {
  auto it = this->page_table_.begin();
  for (; it != this->page_table_.end(); it++) {
    this->FlushPage(it->first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return true;
  } else {
    if (pages_[it->first].pin_count_ != 0) {
      latch_.unlock();
      return false;
    } else {
      free_list_.push_back(it->second);
      this->replacer_->node_store_[it->second].is_evictable_ = false;
      pages_[it->first].pin_count_ = 1;
      pages_[it->first].ResetMemory();
      DeallocatePage(it->first);
      page_table_.erase(it->first);
      latch_.unlock();
      return true;
    }
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  this->latch_.lock();
  // int flag=1;
  // auto it=this->page_table_.begin();
  // for(;it!=this->page_table_.end();it++)
  // {
  //   if(this->pages_[it->first].pin_count_==0)//这里从下文看可以用replacer的evitable来判断，但是我感觉pin和evitable是一样的
  //   {
  //     flag=0;
  //     break;
  //   }
  // }
  frame_id_t frame1 = 0;
  auto it2 = this->page_table_.find(page_id);
  if (it2 == page_table_.end()) {
    if (!this->free_list_.empty()) {
      // NOT null,fetch frame and duqu from disk
      frame1 = this->free_list_.front();
      free_list_.pop_front();
      page_id_t temp_id;
      for (auto &pair : this->page_table_) {
        if (pair.second == frame1) {
          temp_id = pair.first;  // fan hui page_id
        }
      }
      pages_[temp_id].pin_count_++;
      this->replacer_->SetEvictable(frame1, false);
      this->replacer_->RecordAccess(frame1);
      this->disk_scheduler_->disk_manager_->ReadPage(temp_id, this->pages_[temp_id].data_);
      page_id = temp_id;
      this->latch_.unlock();
      return BasicPageGuard(this, &pages_[temp_id]);
    } else {
      frame_id_t temp = 0;
      if (this->replacer_->Evict(&temp) != 1) {
        this->latch_.unlock();
        return BasicPageGuard();  // 无法fetch页面
      }
      page_id_t temp_id;
      for (auto &pair : this->page_table_) {
        if (pair.second == temp) {
          temp_id = pair.first;  // fan hui page_id
        }
        Page *temp_page = nullptr;
        temp_page = &pages_[temp_id];
        if (this->pages_[temp_id].is_dirty_ == true) {
          this->latch_.unlock();
          this->FlushPage(temp_id);
          this->latch_.lock();
          temp_page->ResetMemory();
        }
        this->disk_scheduler_->disk_manager_->ReadPage(temp_id, this->pages_[temp_id].data_);
        this->replacer_->SetEvictable(temp, false);
        this->replacer_->RecordAccess(temp);
        this->replacer_->node_store_.erase(temp);
        pages_[temp_id].pin_count_++;
        page_id = temp_id;
        latch_.unlock();
        return BasicPageGuard(this, &pages_[temp_id]);
      }
    }
  } else {
    // find!!!
    page_id = it2->first;
    this->replacer_->SetEvictable(it2->second, false);
    this->replacer_->RecordAccess(it2->second);
    pages_[page_id].pin_count_++;
    latch_.unlock();
    return BasicPageGuard(this, &pages_[page_id]);
  }
  latch_.unlock();
  return BasicPageGuard();
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  this->latch_.lock();

  // int flag=1;
  // auto it=this->page_table_.begin();
  // for(;it!=this->page_table_.end();it++)
  // {
  //   if(this->pages_[it->first].pin_count_==0)//这里从下文看可以用replacer的evitable来判断，但是我感觉pin和evitable是一样的
  //   {
  //     flag=0;
  //     break;
  //   }
  // }
  frame_id_t frame1 = 0;
  auto it2 = this->page_table_.find(page_id);
  if (it2 == page_table_.end()) {
    if (!this->free_list_.empty()) {
      // NOT null,fetch frame and duqu from disk
      frame1 = this->free_list_.front();
      free_list_.pop_front();
      page_id_t temp_id=0;
      for (auto &pair : this->page_table_) {
        if (pair.second == frame1) {
          temp_id = pair.first;  // fan hui page_id
        }
      }
      pages_[temp_id].pin_count_++;
      this->replacer_->SetEvictable(frame1, false);
      this->replacer_->RecordAccess(frame1);
      this->disk_scheduler_->disk_manager_->ReadPage(temp_id, this->pages_[temp_id].data_);
      page_id = temp_id;
      this->latch_.unlock();
      return BasicPageGuard(this, &pages_[temp_id]).UpgradeRead();
    } else {
      frame_id_t temp = 0;
      if (this->replacer_->Evict(&temp) != 1) {
        this->latch_.unlock();
        return BasicPageGuard().UpgradeRead();  // 无法fetch页面
      }
      page_id_t temp_id;
      for (auto &pair : this->page_table_) {
        if (pair.second == temp) {
          temp_id = pair.first;  // fan hui page_id
        }
        Page *temp_page = nullptr;
        temp_page = &pages_[temp_id];
        if (this->pages_[temp_id].is_dirty_ == true) {
          this->latch_.unlock();
          this->FlushPage(temp_id);
          this->latch_.lock();
          temp_page->ResetMemory();
        }
        this->disk_scheduler_->disk_manager_->ReadPage(temp_id, this->pages_[temp_id].data_);
        this->replacer_->SetEvictable(temp, false);
        this->replacer_->RecordAccess(temp);
        this->replacer_->node_store_.erase(temp);
        pages_[temp_id].pin_count_++;
        page_id = temp_id;
        latch_.unlock();
        return BasicPageGuard(this, &pages_[temp_id]).UpgradeRead();
      }
    }
  } else {
    // std::cout<<"f2"<<std::endl;
    // find!!!
    page_id = it2->first;
    this->replacer_->SetEvictable(it2->second, false);
    this->replacer_->RecordAccess(it2->second);
    // std::cout<<"f3"<<std::endl;
    pages_[page_id].pin_count_++;
    latch_.unlock();
    return BasicPageGuard(this, &pages_[page_id]).UpgradeRead();
  }
  latch_.unlock();
  return BasicPageGuard().UpgradeRead();
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  this->latch_.lock();
  // int flag=1;
  // auto it=this->page_table_.begin();
  // for(;it!=this->page_table_.end();it++)
  // {
  //   if(this->pages_[it->first].pin_count_==0)//这里从下文看可以用replacer的evitable来判断，但是我感觉pin和evitable是一样的
  //   {
  //     flag=0;
  //     break;
  //   }
  // }
  frame_id_t frame1 = 0;
  auto it2 = this->page_table_.find(page_id);
  if (it2 == page_table_.end()) {
    if (!this->free_list_.empty()) {
      // NOT null,fetch frame and duqu from disk
      frame1 = this->free_list_.front();
      free_list_.pop_front();
      page_id_t temp_id;
      for (auto &pair : this->page_table_) {
        if (pair.second == frame1) {
          temp_id = pair.first;  // fan hui page_id
        }
      }
      pages_[temp_id].pin_count_++;
      this->replacer_->SetEvictable(frame1, false);
      this->replacer_->RecordAccess(frame1);
      this->disk_scheduler_->disk_manager_->ReadPage(temp_id, this->pages_[temp_id].data_);
      page_id = temp_id;
      this->latch_.unlock();
      return BasicPageGuard(this, &pages_[temp_id]).UpgradeWrite();
    } else {
      frame_id_t temp = 0;
      if (this->replacer_->Evict(&temp) != 1) {
        this->latch_.unlock();
        return BasicPageGuard().UpgradeWrite();  // 无法fetch页面
      }
      page_id_t temp_id;
      for (auto &pair : this->page_table_) {
        if (pair.second == temp) {
          temp_id = pair.first;  // fan hui page_id
        }
        Page *temp_page = nullptr;
        temp_page = &pages_[temp_id];
        if (this->pages_[temp_id].is_dirty_ == true) {
          this->latch_.unlock();
          this->FlushPage(temp_id);
          this->latch_.lock();
          temp_page->ResetMemory();
        }
        this->disk_scheduler_->disk_manager_->ReadPage(temp_id, this->pages_[temp_id].data_);
        this->replacer_->SetEvictable(temp, false);
        this->replacer_->RecordAccess(temp);
        this->replacer_->node_store_.erase(temp);
        pages_[temp_id].pin_count_++;
        page_id = temp_id;
        latch_.unlock();
        return BasicPageGuard(this, &pages_[temp_id]).UpgradeWrite();
      }
    }
  } else {
    // find!!!
    page_id = it2->first;
    this->replacer_->SetEvictable(it2->second, false);
    this->replacer_->RecordAccess(it2->second);
    pages_[page_id].pin_count_++;
    latch_.unlock();
    return BasicPageGuard(this, &pages_[page_id]).UpgradeWrite();
  }
  latch_.unlock();
  return BasicPageGuard().UpgradeWrite();
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  this->latch_.lock();
  if (this->free_list_.empty() == true) {
    int flag = 1;
    auto it = this->page_table_.begin();
    for (; it != this->page_table_.end(); it++) {
      if (this->pages_[it->first].pin_count_ ==
          0)  //这里从下文看可以用replacer的evitable来判断，但是我感觉pin和evitable是一样的
      {
        flag = 0;
        break;
      }
    }
    if (flag == 1) {
      this->latch_.unlock();
      return BasicPageGuard();
    }
  }
  page_id_t newid = this->AllocatePage();
  frame_id_t temp = 0;
  if (this->free_list_.empty() != true) {
    temp = this->free_list_.front();
    free_list_.pop_front();
    this->page_table_.emplace(newid, temp);
    Page &new_page = pages_[newid];
    // new_page=*new Page();
    // Page &new_page=*new Page();
    // new_page=pages_[newid];
    new_page.is_dirty_ = false;
    new_page.pin_count_ = 1;
    this->replacer_->SetEvictable(temp, false);
    this->replacer_->RecordAccess(temp);
    new_page.page_id_ = newid;
    *page_id = newid;
    // this->disk_scheduler_->disk_manager_ ->WritePage(newid,this->pages_[newid].data_);
    latch_.unlock();
    return BasicPageGuard(this, &pages_[newid]);
  } else {
    if (this->replacer_->Evict(&temp) != 1) {
      this->latch_.unlock();
      return BasicPageGuard();  // 无法创建新页面
    }
    page_id_t temp_id;
    auto pair = page_table_.begin();
    for (; pair != page_table_.end(); pair++) {
      if (pair->second == temp) {
        temp_id = pair->first;  // fan hui page_id
        break;
      }
    }
    Page *temp_page = nullptr;
    temp_page = &pages_[temp_id];
    if (pages_[temp_id].is_dirty_ == true) {
      this->latch_.unlock();
      this->FlushPage(temp_id);
      this->latch_.lock();
      temp_page->ResetMemory();
    }
    this->replacer_->SetEvictable(temp, false);
    this->replacer_->RecordAccess(temp);
    temp_page->page_id_ = newid;
    this->page_table_.erase(temp_id);
    Page &new_page = pages_[newid];
    new_page.is_dirty_ = false;
    new_page.pin_count_ = 1;
    this->page_table_.emplace(newid, temp);
    this->replacer_->node_store_.erase(temp);  // important!!!
    *page_id = newid;
    this->disk_scheduler_->disk_manager_->WritePage(newid, this->pages_[newid].data_);
    latch_.unlock();
    return BasicPageGuard(this, &pages_[newid]);
  }
latch_.unlock();
  return BasicPageGuard();
}

}  // namespace bustub
