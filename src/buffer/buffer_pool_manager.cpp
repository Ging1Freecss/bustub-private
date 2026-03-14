//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, 2)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/** @brief return victim page frame id */
auto BufferPoolManager::ClaimFrame() -> std::optional<frame_id_t> {
  std::optional<frame_id_t> opt_fid{std::nullopt};

  if (!free_frames_.empty()) {  // check free frames first
    opt_fid = free_frames_.front();
    free_frames_.pop_front();
  } else {  // evict frame from lru-r
    opt_fid = replacer_->Evict();

    if (!opt_fid) return std::nullopt;

    std::shared_ptr<FrameHeader> victim_ptr = frames_[opt_fid.value()];

    if (victim_ptr->is_dirty_) {
      DiskScheduler::DiskSchedulerPromise disk_pr;
      std::future<bool> fr = disk_pr.get_future();

      bool is_write = true;
      char *data = victim_ptr->GetDataMut();

      DiskRequest disk_rq{is_write, data, victim_ptr->page_id_, std::move(disk_pr)};
      std::vector<DiskRequest> input;
      input.push_back(std::move(disk_rq));
      disk_scheduler_->Schedule(input);

      if (!fr.get()) {
        replacer_->RecordAccess(opt_fid.value());
        replacer_->SetEvictable(opt_fid.value(), true);
        return std::nullopt;
      }

      victim_ptr->is_dirty_ = false;
    }

    page_table_.erase(victim_ptr->page_id_);
    victim_ptr->page_id_ = INVALID_PAGE_ID;
  }
  return opt_fid;
}
/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  std::unique_lock uk{*bpm_latch_};

  std::optional<frame_id_t> opt_fid{ClaimFrame()};

  if (!opt_fid) return INVALID_PAGE_ID;

  page_id_t page_id = next_page_id_;
  next_page_id_.fetch_add(1);

  frame_id_t fid = opt_fid.value();
  auto frame_ptr = frames_[fid];

  frame_ptr->Reset();  // pin count,is_dirty and data
  frame_ptr->pin_count_ = 0;
  frame_ptr->page_id_ = page_id;
  frame_ptr->is_dirty_ = true;
  page_table_.insert({page_id, fid});

  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  return page_id;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places that a page or a page's metadata could be, and use that to guide you on implementing
 * this function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * You should call `DeallocatePage` in the disk scheduler to make the space available for new pages.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard lk{*bpm_latch_};
  if (page_id == INVALID_PAGE_ID) return false;

  auto it = page_table_.find(page_id);

  if (it == page_table_.end()) return true;  // already deleted

  frame_id_t fid{it->second};
  std::shared_ptr<FrameHeader> frame_ptr{frames_[fid]};

  if (frame_ptr->pin_count_ > 0) return false;

  bool status = replacer_->Remove(fid);

  if (!status) return false;
  page_table_.erase(page_id);

  frame_ptr->Reset();
  frame_ptr->page_id_ = INVALID_PAGE_ID;

  disk_scheduler_->DeallocatePage(page_id);
  free_frames_.push_back(fid);

  return true;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> std::shared_ptr<FrameHeader> {
  auto it = page_table_.find(page_id);

  if (it != page_table_.end()) {
    frame_id_t fid = it->second;
    std::shared_ptr<FrameHeader> frame_ptr = this->frames_[fid];
    frame_ptr->pin_count_.fetch_add(1);  // frame is acquired

    // make sure to not evict it after inserting it in lru-r
    this->replacer_->RecordAccess(fid, access_type);
    this->replacer_->SetEvictable(fid, false);

    return frame_ptr;
  }

  std::optional<frame_id_t> opt_fid{std::nullopt};

  // check if free frame exist
  if (!free_frames_.empty()) {
    opt_fid = free_frames_.front();
    free_frames_.pop_front();
  } else {
    // if not , then evict frame from lru-r
    opt_fid = replacer_->Evict();

    if (!opt_fid) return nullptr;

    std::shared_ptr<FrameHeader> victim_ptr = this->frames_[opt_fid.value()];

    // write to disk logic
    if (victim_ptr->is_dirty_) {
      DiskScheduler::DiskSchedulerPromise disk_pr;
      std::future<bool> fr = disk_pr.get_future();

      bool is_write = true;
      char *data = victim_ptr->GetDataMut();

      DiskRequest disk_rq{is_write, data, victim_ptr->page_id_, std::move(disk_pr)};
      std::vector<DiskRequest> input;
      input.push_back(std::move(disk_rq));
      disk_scheduler_->Schedule(input);

      bool status = fr.get();
      if (!status) {
        fprintf(stderr, "DEBUG: CRITICAL! Disk write failed for page %d\n", victim_ptr->page_id_);
        return nullptr;
      }
      // fprintf(stderr, "DEBUG: Flushed dirty page %d during eviction\n", victim_ptr->page_id_);
      victim_ptr->is_dirty_ = false;
    }
    page_table_.erase(victim_ptr->page_id_);  // since we evict the frame
  }

  frame_id_t fid{opt_fid.value()};

  page_table_[page_id] = fid;
  // update frame header
  std::shared_ptr<FrameHeader> frame_ptr = this->frames_[opt_fid.value()];
  frame_ptr->pin_count_ = 1;
  frame_ptr->is_dirty_ = false;
  frame_ptr->page_id_ = page_id;

  DiskScheduler::DiskSchedulerPromise disk_pr;
  std::future<bool> fr = disk_pr.get_future();

  bool is_write = false;  // just read the data

  DiskRequest disk_rq{is_write, frame_ptr->GetDataMut(), page_id, std::move(disk_pr)};

  std::vector<DiskRequest> input;
  input.push_back(std::move(disk_rq));
  disk_scheduler_->Schedule(input);

  // failed to read data -> reset
  if (!fr.get()) {
    page_table_.erase(page_id);  // delete here as error in reading data from the page id, so not to store currupt data

    // reset frame header
    frame_ptr->is_dirty_ = false;
    frame_ptr->page_id_ = INVALID_PAGE_ID;
    frame_ptr->pin_count_ = 0;

    free_frames_.push_back(fid);  // put back the frame to free frame as no use now
    return nullptr;
  }

  // after getting the frame , set lru-r to record the frame and not evict it
  replacer_->RecordAccess(fid, access_type);
  replacer_->SetEvictable(fid, false);

  return frame_ptr;
}
/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if
 * needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire
 * either a `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data,
 * which ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both
 * immutable and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data
 * however they want. If a user wants to have multiple threads reading the page at the same time, they must acquire
 * a `ReadPageGuard` with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are three main cases that you will have to implement. The first two are relatively simple: one is when
 * there is plenty of available memory, and the other is when we don't actually need to perform any additional I/O.
 * Think about what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper
 * functions useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of
 * memory) returns `std::nullopt`; otherwise, returns a `WritePageGuard` ensuring exclusive and mutable access to a
 * page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  std::unique_lock<std::mutex> uk{*bpm_latch_};

  auto frame_ptr = FetchPage(page_id, access_type);

  if (!frame_ptr) {
    return std::nullopt;
  }

  uk.unlock();  // important to unlock it here as pagegaurd lock page mutex in constructor and in destructor it lock
                // bpm, can lead to deadlock
  return WritePageGuard{page_id, frame_ptr, replacer_, bpm_latch_, disk_scheduler_};
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  std::unique_lock<std::mutex> uk{*bpm_latch_};

  auto frame_ptr = FetchPage(page_id, access_type);

  if (!frame_ptr) {
    return std::nullopt;
  }

  uk.unlock();
  return ReadPageGuard{page_id, frame_ptr, replacer_, bpm_latch_, disk_scheduler_};
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::print(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::print(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk unsafely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should not take a lock on the page in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto table_ptr = page_table_.find(page_id);

  if (table_ptr == page_table_.end()) {
    return false;
  }

  frame_id_t fid = table_ptr->second;
  std::shared_ptr<FrameHeader> frame_ptr = frames_[fid];

  DiskScheduler::DiskSchedulerPromise disk_pr;
  std::future<bool> fr = disk_pr.get_future();

  bool is_write = true;
  char *data = frame_ptr->GetDataMut();

  DiskRequest disk_rq{is_write, data, frame_ptr->page_id_, std::move(disk_pr)};
  std::vector<DiskRequest> input;
  input.push_back(std::move(disk_rq));
  disk_scheduler_->Schedule(input);

  if (!fr.get()) return false;

  frame_ptr->is_dirty_ = false;

  return true;
}

/**
 * @brief Flushes a page's data out to disk safely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should take a lock on the page in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `Flush` in the page guards, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard lk{*bpm_latch_};
  return FlushPageUnsafe(page_id);
}

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 *
 * You should not take locks on the pages in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  for (auto &[page_id, frame_id] : page_table_) {
    FlushPageUnsafe(page_id);
  }
}

/**
 * @brief Flushes all page data that is in memory to disk safely.
 *
 * You should take locks on the pages in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  std::lock_guard lk{*bpm_latch_};
  FlushAllPagesUnsafe();
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists; otherwise, `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::lock_guard lk{*bpm_latch_};

  auto it = page_table_.find(page_id);

  if (it == page_table_.end()) return std::nullopt;

  frame_id_t fid = it->second;
  std::shared_ptr<FrameHeader> frame_ptr = frames_[fid];

  return frame_ptr->pin_count_.load();
}

}  // namespace bustub
