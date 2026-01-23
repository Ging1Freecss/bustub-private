//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>
#include "buffer/arc_replacer.h"
#include "common/macros.h"

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id), frame_(frame), replacer_(replacer), bpm_latch_(bpm_latch), disk_scheduler_(disk_scheduler) {
  //
  BUSTUB_ASSERT(frame_ != nullptr, "FrameHeader is null");
  BUSTUB_ASSERT(replacer_ != nullptr, "Replacer is null");
  BUSTUB_ASSERT(bpm_latch_ != nullptr, "bpm_latch_ is null");
  BUSTUB_ASSERT(disk_scheduler_ != nullptr, "disk_scheduler_ is null");

  is_valid_ = true;
  frame_->rwlatch_.lock_shared();
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
    : page_id_{that.page_id_},
      frame_{std::move(that.frame_)},
      replacer_{std::move(that.replacer_)},
      bpm_latch_{std::move(that.bpm_latch_)},
      disk_scheduler_{std::move(that.disk_scheduler_)},
      is_valid_{that.is_valid_} {
  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }
  this->Drop();  // drop the current resource held by this
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;

  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;

  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Flush() {
  std::scoped_lock lk{*bpm_latch_};
  DiskScheduler::DiskSchedulerPromise disk_pr;
  std::future<bool> fr = disk_pr.get_future();

  bool is_write = true;
  char *data = frame_->GetDataMut();
  page_id_t page_id = page_id_;

  DiskRequest disk_rq{is_write, data, page_id, std::move(disk_pr)};
  std::vector<DiskRequest> input;
  input.push_back(std::move(disk_rq));
  disk_scheduler_->Schedule(input);

  bool status = fr.get();
  // BUSTUB_ASSERT(status,"error when flushing the data");

  if (status) {
    frame_->is_dirty_ = false;
  }
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Drop() {
  if (!is_valid_ || (frame_ == nullptr)) return;

  frame_->rwlatch_.unlock_shared();
  std::scoped_lock<std::mutex> lk_bpm{*bpm_latch_};

  frame_->pin_count_.fetch_sub(1);

  if (frame_->pin_count_ == 0) {
    replacer_->SetEvictable(frame_->frame_id_, true);
  }

  is_valid_ = false;
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id), frame_(frame), replacer_(replacer), bpm_latch_(bpm_latch), disk_scheduler_(disk_scheduler) {
  BUSTUB_ASSERT(frame_ != nullptr, "FrameHeader is null");
  BUSTUB_ASSERT(replacer_ != nullptr, "Replacer is null");
  BUSTUB_ASSERT(bpm_latch_ != nullptr, "bpm_latch_ is null");
  BUSTUB_ASSERT(disk_scheduler_ != nullptr, "disk_scheduler_ is null");

  is_valid_ = true;
  frame_->rwlatch_.lock();
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
    : page_id_{that.page_id_},
      frame_{std::move(that.frame_)},
      replacer_{std::move(that.replacer_)},
      bpm_latch_{std::move(that.bpm_latch_)},
      disk_scheduler_{std::move(that.disk_scheduler_)},
      is_valid_{that.is_valid_} {
  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == (&that)) {
    return *this;
  }
  this->Drop();  // drop the current resource held by this
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;

  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");

  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  frame_->is_dirty_ =
      true;  // really really important, as when cailling this func , mean the prog explictly want to modify
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Flush() {
  std::scoped_lock lk{*bpm_latch_};
  DiskScheduler::DiskSchedulerPromise disk_pr;
  std::future<bool> fr = disk_pr.get_future();

  bool is_write = true;
  char *data = frame_->GetDataMut();
  page_id_t page_id = page_id_;

  DiskRequest disk_rq{is_write, data, page_id, std::move(disk_pr)};
  std::vector<DiskRequest> input;
  input.push_back(std::move(disk_rq));
  disk_scheduler_->Schedule(input);

  bool status = fr.get();
  // BUSTUB_ASSERT(status,"error when flushing the data");

  if (status) {
    frame_->is_dirty_ = false;
  }
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Drop() {
  if (!is_valid_) return;

  if (frame_ == nullptr) return;

  frame_->rwlatch_.unlock();
  std::scoped_lock lk{*bpm_latch_};

  frame_->pin_count_.fetch_sub(1);

  if (frame_->pin_count_ == 0) {
    replacer_->SetEvictable(frame_->frame_id_, true);
  }

  is_valid_ = false;
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
