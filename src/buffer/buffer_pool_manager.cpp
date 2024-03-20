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
  pages_ = new Page[pool_size_+20];

  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }


// auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * 
// {
//   this->latch_.lock();
//   if(!free_list_.empty())
//   {
//     page_id_t newid=this->AllocatePage();
//     frame_id_t temp=0;
//     temp=this->free_list_.front();
//     free_list_.pop_front();
//     this->page_table_.emplace(newid, temp);
//     Page& new_page=pages_[newid];
//     new_page.is_dirty_=false;
//     new_page.pin_count_=0;
//     new_page.page_id_=newid;
//     *page_id=newid;
//     this->disk_scheduler_->disk_manager_ ->WritePage(newid,this->pages_[newid].data_);
//     latch_.unlock();
//     return &new_page;
//   }
//   return nullptr;
// }
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * 
{
  this->latch_.lock();
  std::cout<<this->free_list_.empty()<<std::endl;
  std::cout<<this->free_list_.empty()<<std::endl;
  if(this->free_list_.empty()==true)
  {
    int flag=1;
    auto it=this->page_table_.begin();
    for(;it!=this->page_table_.end();it++)
    {
      // if(this->pages_[it->first].pin_count_==0)//这里从下文看可以用replacer的evitable来判断，但是我感觉pin和evitable是一样的
      // {
      //   flag=0;
      //   break;
      // }
      if(this->pages_[it->first].pin_count_==0)//这里从下文看可以用replacer的evitable来判断，但是我感觉pin和evitable是一样的
      {
        flag=0;
        break;
      }
    }
    if(flag==1)
    {
      this->latch_.unlock();
      // std::cout<<"fanhui"<<std::endl;
      return nullptr;
    }
  }
  page_id_t newid=this->AllocatePage();
  // std::cout<<" _____________"<<newid<<"_______________________________"<<std::endl;
  // std::cout<<" _____________"<<newid<<"_______________________________"<<std::endl;
  frame_id_t temp=0;
  if(this->free_list_.empty()!=true)
  {
    std::cout<<" free_list_.front();"<<free_list_.front()<<std::endl;
    temp=this->free_list_.front();
    free_list_.pop_front();
    this->page_table_.emplace(newid, temp);
    // Page& new_page=pages_[newid];
    // std::shared_ptr<Page> new_page = std::make_shared<Page>();
    // pages_[newid]=new Page();
    Page &new_page=pages_[newid];
    new_page=*new Page();
    new_page.is_dirty_=false;
    new_page.pin_count_=1;
    this->replacer_->SetEvictable(temp,false);
    this->replacer_->RecordAccess(temp);
    new_page.page_id_=newid;
    *page_id=newid;
    // this->disk_scheduler_->disk_manager_ ->WritePage(newid,this->pages_[newid].data_);
    latch_.unlock();
    std::cout<<"shi"<<&pages_[newid]<<std::endl;
    std::cout<<"shi"<<&pages_[newid]<<std::endl;
    return &pages_[newid];
  }
  else
  {
    if (this->replacer_->Evict(&temp) != 1) {
      this->latch_.unlock();
      std::cout<<"fanhui3"<<std::endl;
      return nullptr; // 无法创建新页面
    }
    std::cout<<"temp"<<temp<<std::endl;
    page_id_t temp_id;
    std::cout<<"tempframe"<<page_table_[0]<<std::endl;
    std::cout<<"tempframe"<<page_table_[1]<<std::endl;
    auto pair=page_table_.begin();
    for (;pair!=page_table_.end();pair++) {
      std::cout<<"pair_.first"<<pair->first<<std::endl;
       std::cout<<"pair_.second"<<pair->second<<std::endl;
        std::cout<<"temp"<<temp<<std::endl;
    if (pair->second==temp) {
      std::cout<<"yes"<<std::endl;
      temp_id=pair->first;//fan hui page_id
      break;
    }
    }
    Page* temp_page=nullptr;
    std::cout<<"temp_id"<<temp_id<<std::endl;
    temp_page=&pages_[temp_id];
    std::cout<<"temp_page"<<temp_page<<std::endl;
    std::cout<<"temp_page"<<temp_page->page_id_<<std::endl;
    if(pages_[temp_id].is_dirty_==true)
    {
      this->latch_.unlock();
      this->FlushPage(temp_id);
      this->latch_.lock();
      temp_page->ResetMemory();
    }
      this->replacer_->SetEvictable(temp,false);
      this->replacer_->RecordAccess(temp);
      temp_page->page_id_=newid;
      // delete &pages_[temp_id];
      // temp_id=INVALID_PAGE_ID;
      // pages_.erase(temp_id);buzhidaoyaobuyaochuli
      Page& new_page=pages_[newid];
      // new_page=*new Page();
      new_page.is_dirty_=false;
      // new_page.pin_count_=0;
      new_page.pin_count_=1;
      // this->page_table_.erase(temp_id);
      this->page_table_.emplace(newid, temp);
      // this->replacer_->node_store_[temp].is_evictable_=false;
      this->replacer_->node_store_.erase(temp);
      *page_id=newid;
      this->disk_scheduler_->disk_manager_ ->WritePage(newid,this->pages_[newid].data_);
      latch_.unlock();
          std::cout<<"shi"<<&pages_[newid]<<std::endl;
    std::cout<<"shi"<<&pages_[newid]<<std::endl;
      return &pages_[newid];
  }

return nullptr;
}





auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * 
{
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
  frame_id_t frame1=0;
  auto it2=this->page_table_.find(page_id);
  if(it2==page_table_.end())
  {
    if(!this->free_list_.empty())
    {
      //NOT null,fetch frame and duqu from disk
      frame1=this->free_list_.front();
      free_list_.pop_front();
      page_id_t temp_id;
      for (auto& pair:this->page_table_) {
      if (pair.second==frame1) {
        temp_id=pair.first;//fan hui page_id
    }
      }
    pages_[temp_id].pin_count_++;
    this->replacer_->SetEvictable(frame1,false);
    this->replacer_->RecordAccess(frame1);
    this->disk_scheduler_->disk_manager_ ->ReadPage(temp_id,this->pages_[temp_id].data_);
    page_id=temp_id;
    this->latch_.unlock();
    return &pages_[temp_id];
    }
    else 
    {
      frame_id_t temp=0;
      if (this->replacer_->Evict(&temp) != 1) 
      {
      this->latch_.unlock();
      return nullptr; // 无法fetch页面
      } 
      page_id_t temp_id;
    for (auto& pair:this->page_table_) {
    if (pair.second==temp) {
      temp_id=pair.first;//fan hui page_id
    }
    Page* temp_page=nullptr;
    temp_page=&pages_[temp_id];
    if(this->pages_[temp_id].is_dirty_==true)
    {
      this->latch_.unlock();
      this->FlushPage(temp_id);
      this->latch_.lock();
      temp_page->ResetMemory();
    }
    this->disk_scheduler_->disk_manager_ ->ReadPage(temp_id,this->pages_[temp_id].data_);
      this->replacer_->SetEvictable(temp,false);
      this->replacer_->RecordAccess(temp);
      pages_[temp_id].pin_count_++;
      page_id=temp_id;
      latch_.unlock();
      return &pages_[temp_id];
    }

  }
  }
  else
  {
    //find!!!
    page_id=it2->first;
    this->replacer_->SetEvictable(it2->second,false);
    this->replacer_->RecordAccess(it2->second);
    pages_[page_id].pin_count_++;
    latch_.unlock();
    return &pages_[page_id];
  }
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool 
{
  latch_.lock();
  auto it=this->page_table_.find(page_id);
  if(it==page_table_.end())
  {
    std::cout<<"enter0"<<std::endl;
    latch_.unlock();
    return false;
  }
  else if(pages_[page_id].pin_count_==0)
  {
    std::cout<<"enter2"<<std::endl;
    frame_id_t frame1=this->page_table_[page_id];
    this->replacer_->node_store_[frame1].is_evictable_=true;
    latch_.unlock();
    return false;
  }
  else
  {
    std::cout<<"enter1"<<std::endl;
    pages_[page_id].pin_count_=0;
    frame_id_t frame1=this->page_table_[page_id];

    std::cout<<frame1<<std::endl;
    this->replacer_->curr_size_++;
    this->replacer_->RecordAccess(frame1);
    this->replacer_->node_store_[frame1].is_evictable_=true;
    pages_[page_id].is_dirty_=is_dirty;
    latch_.unlock();
    return true;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool 
{
  // this->latch_.lock();
  if(page_id==INVALID_PAGE_ID){
    // this->latch_.unlock();
   return false; 
  }
  // std::unordered_map<page_id_t, frame_id_t> page_table_;用于跟踪缓冲池页面的页表
  auto it=this->page_table_.find(page_id);
  if(it==this->page_table_.end())
  {
    // latch_.unlock();
    return false;
  }
  else 
  {
    this->disk_scheduler_->disk_manager_ ->WritePage(page_id,this->pages_[page_id].data_);
    this->pages_[page_id].is_dirty_=false;
    // latch_.unlock();
    return true;
  }
}

void BufferPoolManager::FlushAllPages()
{
  auto it=this->page_table_.begin();
  for(;it!=this->page_table_.end();it++)
  {
    this->FlushPage(it->first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
