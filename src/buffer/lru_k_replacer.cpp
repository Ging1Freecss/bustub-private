//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard lk{latch_};
  if (curr_size_ == 0) return std::nullopt;

  struct frame_ {
    bool exist{false};
    frame_id_t frame_id;
    size_t difference;

    void set(bool exist_, frame_id_t frame_id_, size_t difference_) {
      exist = exist_;
      frame_id = frame_id_;
      difference = difference_;
    }
  };

  frame_ lessThan_k;
  frame_ equalTo_k;

  for (auto &[frame_id, node] : node_store_) {
    //
    if (!node.is_evictable_) continue;

    if (node.history_.size() < k_) {
      // fifo
      size_t time_stamp = node.history_.front();

      if (!lessThan_k.exist || lessThan_k.difference > time_stamp) {
        lessThan_k.set(true, frame_id, time_stamp);
      }
    } else {
      // kth time stamp
      size_t time_stamp = node.history_.back();
      size_t score = current_timestamp_ - time_stamp;

      if (!equalTo_k.exist || equalTo_k.difference < score) {
        equalTo_k.set(true, frame_id, score);
      }
    }
  }

  if (lessThan_k.exist) {
    Remove(lessThan_k.frame_id);
    return std::optional<frame_id_t>{lessThan_k.frame_id};
  }

  if (equalTo_k.exist) {
    Remove(equalTo_k.frame_id);
    return std::optional<frame_id_t>{equalTo_k.frame_id};
  }

  return std::nullopt;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "frame_id is greater than replacer_size");

  std::lock_guard lk{latch_};

  if (auto it = node_store_.find(frame_id); it != node_store_.end()) {
    if (it->second.history_.size() == it->second.k_) {
      it->second.history_.pop_front();
    }
    it->second.history_.emplace_back(current_timestamp_);

  } else {
    LRUKNode newNode{k_, frame_id};
    newNode.history_.emplace_back(current_timestamp_);
    node_store_.emplace(frame_id, newNode);
  }

  current_timestamp_++;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard lk{latch_};
  auto it = node_store_.find(frame_id);

  if (it == node_store_.end()) return;

  if (!it->second.is_evictable_ && set_evictable) {
    it->second.is_evictable_ = set_evictable;
    curr_size_++;

  } else if (it->second.is_evictable_ && !set_evictable) {
    it->second.is_evictable_ = set_evictable;
    curr_size_--;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  auto it = node_store_.find(frame_id);

  BUSTUB_ASSERT(it != node_store_.end(), "frame id is invalid");

  if (!it->second.is_evictable_) return;

  node_store_.erase(it);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard lk{latch_};
  return curr_size_;
}

}  // namespace bustub
