//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  // r.callback_.set_value(true);
  std::optional<DiskRequest> temp{std::move(r)};
  this->request_queue_.Put(std::move(temp));
}
void DiskScheduler::StartWorkerThread() {
  while (1) {
    std::optional<DiskRequest> temp = this->request_queue_.Get();
    if (!temp.has_value()) {
      break;
    }
    if (temp->is_write_ == true) {
      this->disk_manager_->WritePage(temp->page_id_, temp->data_);
      temp->callback_.set_value(true);
    } else {
      this->disk_manager_->ReadPage(temp->page_id_, temp->data_);
      temp->callback_.set_value(true);
    }
  }
}
}  // namespace bustub
