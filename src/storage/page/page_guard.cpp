#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  that.page_ = nullptr;
  that.bpm_ = nullptr;
}
void BasicPageGuard::Drop() {
  if(page_==nullptr)
  {
    return;
  }
  this->bpm_->UnpinPage(
      this->page_->page_id_,
      this->is_dirty_);  /// zhegyinggaishiguard  de mudi 页面防护可确保在相应对象超出范围后立即对其进行调用unpin
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  return *this;
}

BasicPageGuard::~BasicPageGuard()
{
  // Drop();
}; 
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  page_->RLatch();
  ReadPageGuard temp = ReadPageGuard(this->bpm_, this->page_);
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
  return temp;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  // page_->WUnlatch();
  page_->WLatch();
  WritePageGuard temp = WritePageGuard(this->bpm_, this->page_);
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
  return temp;
}
ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {
  this->guard_.bpm_ = bpm;
  this->guard_.page_ = page;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  this->guard_ = BasicPageGuard(std::move(that.guard_));
  that.guard_.page_ = nullptr;
  that.guard_.bpm_ = nullptr;
  that.guard_.is_dirty_ = false;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->guard_.Drop();
  this->guard_ = BasicPageGuard(std::move(that.guard_));
  return *this;
}

void ReadPageGuard::Drop() {
  guard_.page_->RUnlatch();
  this->guard_.Drop();  // 执行释放 latch 的操作
}

ReadPageGuard::~ReadPageGuard() 
{
this->guard_.Drop();
}

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {
  this->guard_.bpm_ = bpm;
  this->guard_.page_ = page;
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  this->guard_ = BasicPageGuard(std::move(that.guard_));
  that.guard_.page_ = nullptr;
  that.guard_.bpm_ = nullptr;
  that.guard_.is_dirty_ = false;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  // this->guard_.Drop();
  this->guard_ = BasicPageGuard(std::move(that.guard_));
  return *this;
}

void WritePageGuard::Drop() {
  dropped_ = true;
  guard_.page_->WUnlatch();
  this->guard_.Drop();  // 执行释放 latch 的操作
}

WritePageGuard::~WritePageGuard()
{
  // guard_.page_->WUnlatch();
  this->guard_.Drop();
        // if (!dropped_) {
        //     guard_.page_->WUnlatch();
        //     // guard_.Drop();
        // }
        // else{
        //   // guard_.Drop();
        // }
}

}  // namespace bustub
