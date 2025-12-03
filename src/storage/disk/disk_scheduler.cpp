//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <vector>
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // Spawn the background thread
  background_thread_.emplace([this] { this->StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param requests The requests to be scheduled.
 */
void DiskScheduler::Schedule(std::vector<DiskRequest> &requests) {
  for (auto &request : requests) {
    request_queue_.Put(std::optional<DiskRequest>{std::move(request)});
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() {
  while (true) {
    // c++ 17 does not have and_then and or_else
    // auto opt = request_queue_.Get()
    //                .and_then([this](DiskRequest &request) {
    //                  auto &[is_write_, data_, page_id_, callback_] = request;

    //                  switch (request.is_write_) {
    //                    case true:
    //                      this->disk_manager_->WritePage(page_id_, data_);
    //                      callback_.set_value(true);
    //                      break;

    //                    case false:
    //                      this->disk_manager_->ReadPage(page_id_, data_);
    //                      callback_.set_value(true);
    //                      break;
    //                  }

    //                  return std::optional<bool>{true};
    //                })
    //                .or_else([]() -> std::optional<bool> { return std::optional<bool>{false}; });

    std::optional<bool> opt;
    auto arg = request_queue_.Get();

    if (arg.has_value()) {
      auto &[is_write_, data_, page_id_, callback_] = arg.value();

      switch (is_write_) {
        case true:
          this->disk_manager_->WritePage(page_id_, data_);
          break;

        case false:
          this->disk_manager_->ReadPage(page_id_, data_);
          break;
      }
      callback_.set_value(true);

      opt = true;
    } else {
      opt = false;
    }

    if (!opt.value()) return;
  }
}

}  // namespace bustub
