//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include <optional>
#include <utility>
#include "buffer/arc_replacer.h"
#include "buffer/traced_buffer_pool_manager.h"
#include "common/config.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();

  return (root_page->root_page_id_ == INVALID_PAGE_ID);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  Context ctx;
  std::optional<ReadPageGuard> read_header_lk{bpm_->ReadPage(header_page_id_, AccessType::Lookup)};
  const BPlusTreeHeaderPage *header_node{read_header_lk->As<BPlusTreeHeaderPage>()};

  if (header_node->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  ctx.root_page_id_ = header_node->root_page_id_;

  page_id_t curr_page_id{ctx.root_page_id_};
  std::optional<ReadPageGuard> curr_lk{std::nullopt};
  std::optional<ReadPageGuard> prev_lk{std::nullopt};

  std::optional<ReadPageGuard> leaf_lk{[&]() -> std::optional<ReadPageGuard> {
    while (true) {
      curr_lk = bpm_->ReadPage(curr_page_id, AccessType::Index);
      if (read_header_lk.has_value()) read_header_lk.reset();

      const BPlusTreePage *curr_page{curr_lk->As<BPlusTreePage>()};

      if (curr_page->IsLeafPage()) {
        return std::move(curr_lk);
      }

      const InternalPage *curr_internal_page{curr_lk->As<InternalPage>()};

      curr_page_id = curr_internal_page->Lookup(key, comparator_);

      prev_lk = std::move(curr_lk.value());
    }
  }()};

  prev_lk.reset();

  const LeafPage *leaf_page{leaf_lk->As<LeafPage>()};
  std::optional<ValueType> opt_value{leaf_page->Lookup(key, comparator_)};

  if (!opt_value) return false;

  (*result).push_back(opt_value.value());

  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry; otherwise, insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false; otherwise, return true.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance. Using the Context is not necessary but advised.

  std::optional<ReadPageGuard> header_read_lk{bpm_->ReadPage(header_page_id_, AccessType::Scan)};

  const BPlusTreeHeaderPage *header_node = header_read_lk->As<BPlusTreeHeaderPage>();

  if (header_node->root_page_id_ == INVALID_PAGE_ID) {
    // create a new node
    Context ctx;
    header_read_lk.reset();
    std::optional<WritePageGuard> header_write_lk{bpm_->WritePage(header_page_id_, AccessType::Index)};
    BPlusTreeHeaderPage *header_node_write = header_write_lk->AsMut<BPlusTreeHeaderPage>();

    if (header_node_write->root_page_id_ != INVALID_PAGE_ID) {
      header_write_lk.reset();

      return Insert(key, value);
    }
    // new page for root
    page_id_t new_root_pid{bpm_->NewPage()};
    std::optional<WritePageGuard> new_root_lk{bpm_->WritePage(new_root_pid, AccessType::Index)};

    // since it is first, it's a leaf node
    LeafPage *leaf_node = new_root_lk->AsMut<LeafPage>();
    leaf_node->Init(leaf_max_size_);
    leaf_node->InsertElement(key, value, comparator_);

    // update ctx and header_page_id

    ctx.root_page_id_ = new_root_pid;

    header_node_write->root_page_id_ = new_root_pid;

    return true;
  }

  //  for optimistic latching case

  {
    Context ctx;
    ctx.root_page_id_ = header_node->root_page_id_;

    std::optional<ReadPageGuard> read_lk{FindLeafOptimistic(key, ctx, Mode::Insert)};
    if (header_read_lk.has_value()) header_read_lk.reset();

    if (read_lk.has_value()) {
      page_id_t leaf_page_pid{read_lk->GetPageId()};
      read_lk.reset();

      {
        // insert element if size < max_size
        std::optional<WritePageGuard> write_lk{bpm_->WritePage(leaf_page_pid)};
        LeafPage *leaf_node{write_lk->AsMut<LeafPage>()};

        if (leaf_node->Lookup(key, comparator_)) {  // check for duplicates
          return false;
        }

        if (leaf_node->GetSize() < leaf_node->GetMaxSize()) {
          leaf_node->InsertElement(key, value, comparator_);
          return true;
        }
      }
    }
  }

  Context ctx;
  // for pessimistic case
  ctx.header_page_ = bpm_->WritePage(header_page_id_, AccessType::Index);
  BPlusTreeHeaderPage *header_node_write{ctx.header_page_->AsMut<BPlusTreeHeaderPage>()};
  ctx.root_page_id_ = header_node_write->root_page_id_;

  std::optional<WritePageGuard> write_lk{FindLeafPessimistic(key, ctx, Mode::Insert)};
  LeafPage *leaf_node = write_lk->AsMut<LeafPage>();

  if (leaf_node->Lookup(key, comparator_)) {
    return false;
  }

  if (leaf_node->GetSize() < leaf_node->GetMaxSize()) {
    leaf_node->InsertElement(key, value, comparator_);
    return true;
  }

  if (leaf_node->GetSize() == leaf_node->GetMaxSize()) {
    // create the receiver page and initialise w.r.t leaf page
    page_id_t receiver_page_id{bpm_->NewPage()};

    std::optional<WritePageGuard> receiver_lk{bpm_->WritePage(receiver_page_id, AccessType::Index)};
    LeafPage *receiver_leaf{receiver_lk->AsMut<LeafPage>()};
    receiver_leaf->Init(leaf_max_size_);

    // split the current node and get the middle key
    KeyType middleKey{
        leaf_node->Split(receiver_leaf, receiver_lk->GetPageId())};  // this split also return the middle key

    if (comparator_(key, middleKey) < 0) {
      leaf_node->InsertElement(key, value, comparator_);
    } else {
      receiver_leaf->InsertElement(key, value, comparator_);
    }
    SplitInternalPage(middleKey, receiver_lk->GetPageId(), ctx);
  }

  return true;
}

// for optimistic part of insert
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafOptimistic(const KeyType &key, Context &ctx, Mode mode) -> std::optional<ReadPageGuard> {
  page_id_t page_id{ctx.root_page_id_};
  std::optional<ReadPageGuard> leaf_read{std::nullopt};

  // find leaf using read latch
  while (true) {
    std::optional<ReadPageGuard> node_lk{bpm_->ReadPage(page_id)};
    const BPlusTreePage *node{node_lk->As<BPlusTreePage>()};

    if (node->IsLeafPage()) {
      leaf_read = std::move(node_lk);
      break;
    }

    const InternalPage *internal_node{node_lk->As<InternalPage>()};
    page_id_t next_page_id{internal_node->Lookup(key, comparator_)};

    switch (mode) {
      case Mode::Insert:
        if (internal_node->GetSize() < internal_node->GetMaxSize()) {
          ctx.read_set_.clear();
          ctx.header_page_ = std::nullopt;
        }
        break;
      case Mode::Delete:
        if (internal_node->GetSize() > internal_node->GetMinSize()) {
          ctx.read_set_.clear();
          ctx.header_page_ = std::nullopt;
        }
        break;
    }

    ctx.read_set_.emplace_back(std::move(node_lk.value()));

    page_id = next_page_id;
  }

  const LeafPage *leaf_node{leaf_read->As<LeafPage>()};

  switch (mode) {
    case Mode::Insert:
      if (leaf_node->GetSize() < leaf_node->GetMaxSize()) {
        return leaf_read;
      }
      break;
    case Mode::Delete:
      if (leaf_node->GetSize() > leaf_node->GetMinSize()) {
        return leaf_read;
      }
      break;
  }

  leaf_read.reset();
  ctx.header_page_ = std::nullopt;
  ctx.read_set_.clear();
  return std::optional<ReadPageGuard>{std::nullopt};
}

/* this is the implementation for pessimistic lock to find leaf it will fail 1 test case for optimistic lock*/
FULL_INDEX_TEMPLATE_ARGUMENTS auto BPLUSTREE_TYPE::FindLeafPessimistic(const KeyType &key, Context &ctx, Mode mode)
    -> std::optional<WritePageGuard> {
  page_id_t page_id{ctx.root_page_id_};

  while (true) {
    std::optional<WritePageGuard> write_lk{bpm_->WritePage(page_id, AccessType::Index)};
    BPlusTreePage *node{write_lk->AsMut<BPlusTreePage>()};

    bool isSafe{false};

    switch (mode) {
      case Mode::Insert:
        if (node->GetSize() < node->GetMaxSize()) {
          isSafe = true;
        }
        break;

      case Mode::Delete:
        if (node->GetSize() > node->GetMinSize()) {
          isSafe = true;
        }
        break;
    }

    if (isSafe) {
      ctx.write_set_.clear();
      ctx.header_page_.reset();
    }

    if (node->IsLeafPage()) {
      return write_lk;
    }

    const InternalPage *internal_node{write_lk->As<InternalPage>()};
    page_id_t next_page_id{internal_node->Lookup(key, comparator_)};

    ctx.write_set_.emplace_back(std::move(write_lk.value()));

    page_id = next_page_id;
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInternalPage(const KeyType &key, const page_id_t &page_id, Context &ctx) {
  KeyType curr_key{key};
  page_id_t curr_page_id{page_id};

  while (!ctx.write_set_.empty()) {
    // ending element are the recent ones
    std::optional<WritePageGuard> write_lk{std::move(ctx.write_set_.back())};
    ctx.write_set_.pop_back();

    // current node
    page_id_t curr_internal_page_id{write_lk->GetPageId()};
    InternalPage *curr_internal_node{write_lk->AsMut<InternalPage>()};

    if (curr_internal_node->GetSize() < curr_internal_node->GetMaxSize()) {
      curr_internal_node->InsertElement(curr_key, curr_page_id, comparator_);
      ctx.write_set_.clear();
      ctx.header_page_ = std::nullopt;
      return;
    }

    // receiver page creation
    page_id_t receiver_page_id{bpm_->NewPage()};
    std::optional<WritePageGuard> receiver_lk{bpm_->WritePage(receiver_page_id, AccessType::Index)};

    InternalPage *receiver_node{receiver_lk->AsMut<InternalPage>()};
    receiver_node->Init(internal_max_size_);

    KeyType key_middle = curr_internal_node->MoveHalfTo(receiver_node, curr_key, curr_page_id, comparator_);
    page_id_t page_middle = receiver_lk->GetPageId(); /* current pageid is receiver because for key>=key_middle
    it corresponds to receiver page , as it is on right side min(receiver keys) > max(current internal node keys)*/

    if (curr_internal_page_id == ctx.root_page_id_) {  // if loop reach root

      // create new page asign it the new root page and insert the key,page id pair
      page_id_t new_root_pid{bpm_->NewPage()};
      std::optional<WritePageGuard> root_lk{bpm_->WritePage(new_root_pid, AccessType::Index)};
      InternalPage *new_root_node{root_lk->AsMut<InternalPage>()};
      new_root_node->Init(internal_max_size_);

      // current key
      new_root_node->SetKeyAt(1, key_middle);
      // left child
      new_root_node->SetValueAt(0, curr_internal_page_id);
      // right child
      new_root_node->SetValueAt(1, page_middle);
      new_root_node->SetSize(2);

      ctx.root_page_id_ = new_root_pid;

      // write to header page
      if (!ctx.header_page_.has_value()) {
        ctx.header_page_ = bpm_->WritePage(header_page_id_);
      }
      BPlusTreeHeaderPage *header_node{ctx.header_page_->AsMut<BPlusTreeHeaderPage>()};
      header_node->root_page_id_ = new_root_pid;

      // clean up
      ctx.write_set_.clear();
      ctx.header_page_ = std::nullopt;
      return;
    }
    curr_key = key_middle;
    curr_page_id = page_middle;
  }

  // if this write_set is empty that means only leaf page exist
  if (ctx.write_set_.empty()) {
    page_id_t new_page_id{bpm_->NewPage()};
    std::optional<WritePageGuard> write_lk{bpm_->WritePage(new_page_id, AccessType::Index)};

    InternalPage *new_root_node{write_lk->AsMut<InternalPage>()};
    new_root_node->Init(internal_max_size_);

    // current key
    new_root_node->SetKeyAt(1, key);
    // left child
    new_root_node->SetValueAt(0, ctx.root_page_id_);
    // right child
    new_root_node->SetValueAt(1, page_id);
    new_root_node->SetSize(2);

    // context class
    ctx.root_page_id_ = new_page_id;

    // header page
    if (!ctx.header_page_.has_value()) {
      ctx.header_page_ = bpm_->WritePage(header_page_id_);
    }
    BPlusTreeHeaderPage *header_node{ctx.header_page_->AsMut<BPlusTreeHeaderPage>()};
    header_node->root_page_id_ = new_page_id;

    // clean up
    ctx.write_set_.clear();
    ctx.header_page_ = std::nullopt;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.

  std::optional<ReadPageGuard> header_reak_lk{bpm_->ReadPage(header_page_id_, AccessType::Scan)};

  if (!bool(header_reak_lk)) {
    return;
  }

  const BPlusTreeHeaderPage *header_node_read = header_reak_lk->As<BPlusTreeHeaderPage>();

  if (header_node_read->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  // optimistic block
  {
    Context ctx;
    ctx.root_page_id_ = header_node_read->root_page_id_;

    std::optional<ReadPageGuard> leaf_read_lk{FindLeafOptimistic(key, ctx, Mode::Delete)};
    if (header_reak_lk.has_value()) header_reak_lk.reset();

    if (leaf_read_lk.has_value()) {
      const LeafPage *leaf_node_read{leaf_read_lk->As<LeafPage>()};

      if (!leaf_node_read->Lookup(key, comparator_)) {
        ctx.read_set_.clear();
        return;
      }

      // get leaf page id and de allocate read latch of leaf , header and read_set of ctx
      page_id_t leaf_pid{leaf_read_lk->GetPageId()};

      if (leaf_pid == ctx.root_page_id_) {  // leaf page is the root page
        leaf_read_lk.reset();

        ctx.header_page_ = bpm_->WritePage(header_page_id_);
        std::optional<WritePageGuard> write_lk{bpm_->WritePage(leaf_pid, AccessType::Index)};

        BPlusTreeHeaderPage *header_node{ctx.header_page_->AsMut<BPlusTreeHeaderPage>()};
        LeafPage *leaf_node{write_lk->AsMut<LeafPage>()};

        if (!leaf_node->DeleteKey(key, comparator_)) {  // if delete return false , key is not found else deleted
          return;
        }

        if (leaf_node->GetSize() == 0) {
          page_id_t leaf_pid{write_lk->GetPageId()};
          write_lk.reset();
          bpm_->DeletePage(leaf_pid);
          header_node->root_page_id_ = INVALID_PAGE_ID;
          ctx.root_page_id_ = INVALID_PAGE_ID;
        }
        return;
      } else if (leaf_node_read->GetSize() > leaf_node_read->GetMinSize()) {
        leaf_read_lk.reset();

        std::optional<WritePageGuard> write_lk{bpm_->WritePage(leaf_pid, AccessType::Index)};
        LeafPage *leaf_node{write_lk->AsMut<LeafPage>()};

        if (leaf_node->GetSize() <= leaf_node->GetMinSize()) {
          write_lk.reset();
        } else {
          leaf_node->DeleteKey(key, comparator_);  // if delete return false , key is not found else deleted
          return;
        }
      }
    }
  }
  Context ctx;
  // get write latch to both header page and leaf page
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  BPlusTreeHeaderPage *header_node{ctx.header_page_->AsMut<BPlusTreeHeaderPage>()};
  ctx.root_page_id_ = header_node->root_page_id_;
  std::optional<WritePageGuard> write_lk{FindLeafPessimistic(key, ctx, Mode::Delete)};

  LeafPage *leaf_node{write_lk->AsMut<LeafPage>()};

  if (!leaf_node->DeleteKey(key, comparator_)) {  // if delete return true
    return;
  }
  // root is leaf
  if (write_lk->GetPageId() == ctx.root_page_id_) {
    if (leaf_node->GetSize() == 0) {  // then delete the page and invalid the header_node root page id

      // header_page recheck important
      if (!ctx.header_page_.has_value()) {
        ctx.header_page_ = bpm_->WritePage(header_page_id_);
      }
      BPlusTreeHeaderPage *header_node = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();

      page_id_t leaf_pid{write_lk->GetPageId()};
      write_lk.reset();

      // de-allocate
      bpm_->DeletePage(leaf_pid);
      header_node->root_page_id_ = INVALID_PAGE_ID;
    }

    return;
  }

  /* at this point ctx.write_set_ has parent of leaf page at ctx.write_set_.back() */
  std::optional<WritePageGuard> leaf_parent_lk{std::move(ctx.write_set_.back())};
  ctx.write_set_.pop_back();

  // fixing leaf
  InternalPage *leaf_parent_node{leaf_parent_lk->AsMut<InternalPage>()};
  int child_idx{leaf_parent_node->ValueIndex(write_lk->GetPageId(), comparator_)};
  page_id_t child_pid{write_lk->GetPageId()};

  std::pair<page_id_t, DeleteOperation> fix_leaf_op{
      FixedLeafAfterDelete(leaf_parent_node, leaf_node, child_idx, child_pid)};

  write_lk.reset();  // unlock as we may later merge, delete the page
  switch (fix_leaf_op.second) {
    case DeleteOperation::None:
      break;
    case DeleteOperation::Borrow:
      break;
    case DeleteOperation::Merge:

      bpm_->DeletePage(fix_leaf_op.first);
      break;
  }

  FixedInternalAfterDelete(std::move(leaf_parent_lk), leaf_parent_node, ctx);
  return;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FixedInternalAfterDelete(std::optional<WritePageGuard> &&child_lk, InternalPage *child,
                                              Context &ctx) {
  while (!ctx.write_set_.empty()) {
    if ((child->GetSize() >= child->GetMinSize()) || (child_lk->GetPageId() == ctx.root_page_id_)) {
      break;
    }
    // parent leaf
    std::optional<WritePageGuard> parent_lk{std::move(ctx.write_set_.back())};
    ctx.write_set_.pop_back();

    InternalPage *parent_node{parent_lk->AsMut<InternalPage>()};

    int child_idx{parent_node->ValueIndex(child_lk->GetPageId(), comparator_)};
    int right_idx = child_idx + 1;
    int left_idx = child_idx - 1;

    // borrow from right child
    std::optional<WritePageGuard> right_lk{std::nullopt};

    if (right_idx < parent_node->GetSize()) {
      right_lk = std::move(bpm_->WritePage(parent_node->ValueAt(right_idx)));
    }

    InternalPage *right_node{nullptr};

    if (right_lk.has_value()) {
      right_node = right_lk->AsMut<InternalPage>();
    }

    if ((right_node != nullptr) && (right_node->GetSize() > right_node->GetMinSize())) {
      BorrowInternalPage(parent_node, right_node, child, right_idx, child_idx);
      return;
    }

    // check for merge , in merge alwys delete the right child
    if ((right_node != nullptr) && right_node->GetSize() <= right_node->GetMinSize()) {
      MergeInternalPage(parent_node, right_node, child, right_idx, child_idx);

      // delete right node
      page_id_t right_pid{right_lk->GetPageId()};
      right_lk.reset();

      // move the parent for next iteration
      child_lk = std::move(parent_lk);
      child = parent_node;

      // delete the right child
      bpm_->DeletePage(right_pid);
      continue;
    }

    right_lk.reset();
    // borrow from left child
    std::optional<WritePageGuard> left_lk{std::nullopt};

    if (left_idx >= 0) {
      left_lk = std::move(bpm_->WritePage(parent_node->ValueAt(left_idx)));
    }

    InternalPage *left_node{nullptr};

    if (left_lk.has_value()) {
      left_node = left_lk->AsMut<InternalPage>();
    }

    if ((left_node != nullptr) && (left_node->GetSize() > left_node->GetMinSize())) {
      BorrowInternalPage(parent_node, left_node, child, left_idx, child_idx);
      return;
    }

    if ((left_node != nullptr) && left_node->GetSize() <= left_node->GetMinSize()) {
      MergeInternalPage(parent_node, left_node, child, left_idx, child_idx);

      //  move the parent for next iteration
      page_id_t child_pid{child_lk->GetPageId()};
      child_lk.reset();
      child_lk = std::move(parent_lk);
      child = parent_node;

      // delete the right child
      bpm_->DeletePage(child_pid);
      continue;
    }
  }

  // if child reach the root node
  /*
  if root page has a size of one , no point having it as we can simply make the child node root page and
  reduce the size of bplustree , case happen when
  key array : |invalid key| |key|
  page array : |page 1| |page 2|
  when merge |key| will be deleted so only invalid and one element in page array remains
  */
  if (child_lk.has_value() && child_lk->GetPageId() == ctx.root_page_id_ && child->GetSize() == 1) {
    if (!ctx.header_page_.has_value()) {
      ctx.header_page_ = bpm_->WritePage(header_page_id_);
    }
    BPlusTreeHeaderPage *header_node{ctx.header_page_->AsMut<BPlusTreeHeaderPage>()};
    page_id_t new_root_pid{child->ValueAt(0)};

    // NEW: Check if the new root will be an empty leaf
    std::optional<WritePageGuard> new_root_guard = bpm_->WritePage(new_root_pid);
    auto *new_root_node = new_root_guard->AsMut<BPlusTreePage>();

    if (new_root_node->IsLeafPage() && new_root_node->GetSize() == 0) {
      // Tree is now completely empty
      header_node->root_page_id_ = INVALID_PAGE_ID;
      ctx.root_page_id_ = INVALID_PAGE_ID;
      new_root_guard.reset();  // drop lock before deleting
      bpm_->DeletePage(new_root_pid);
    } else {
      header_node->root_page_id_ = new_root_pid;
      ctx.root_page_id_ = new_root_pid;
    }
    // drop lock and delete the old internal root
    page_id_t old_root_pid{child_lk->GetPageId()};
    child_lk.reset();
    bpm_->DeletePage(old_root_pid);
  }

  return;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FixedLeafAfterDelete(InternalPage *parent, LeafPage *child, int child_idx, page_id_t child_pid)
    -> std::pair<page_id_t, DeleteOperation> {
  if (child->GetSize() >= child->GetMinSize()) {
    return std::pair{INVALID_PAGE_ID, DeleteOperation::None};
  }
  std::optional<WritePageGuard> right_child{std::nullopt};
  // check right child
  if (child_idx + 1 < parent->GetSize()) {
    right_child = std::move(bpm_->WritePage(parent->ValueAt(child_idx + 1)));
  }

  LeafPage *right_node{nullptr};

  if (right_child.has_value()) {
    right_node = right_child->AsMut<LeafPage>();
  }

  // check for borrowing from right child
  if ((right_node != nullptr) && (right_node->GetSize() > right_node->GetMinSize())) {
    BorrowLeafPage(parent, right_node, child, child_idx + 1, child_idx);
    return std::pair{INVALID_PAGE_ID, DeleteOperation::Borrow};
  }

  // check for merge
  // in merging right child data is deleted always

  // merging right child
  if ((right_node != nullptr) && right_node->GetSize() <= right_node->GetMinSize()) {
    MergeLeafPage(parent, right_node, child, child_idx + 1, child_idx);
    return std::pair{right_child->GetPageId(), DeleteOperation::Merge};
  }

  right_child.reset();
  // check for left child
  std::optional<WritePageGuard> left_child{std::nullopt};

  if (child_idx - 1 >= 0) {
    left_child = std::move(bpm_->WritePage(parent->ValueAt(child_idx - 1)));
  }

  LeafPage *left_node{nullptr};

  if (left_child.has_value()) {
    left_node = left_child->AsMut<LeafPage>();
  }

  // check for borrowing from left child
  if ((left_node != nullptr) && (left_node->GetSize() > left_node->GetMinSize())) {
    BorrowLeafPage(parent, left_node, child, child_idx - 1, child_idx);
    return std::pair{INVALID_PAGE_ID, DeleteOperation::Borrow};
  }

  // merging left child
  if ((left_node != nullptr) && left_node->GetSize() <= left_node->GetMinSize()) {
    MergeLeafPage(parent, left_node, child, child_idx - 1, child_idx);
    return std::pair{child_pid, DeleteOperation::Merge};
  }

  return std::pair{INVALID_PAGE_ID, DeleteOperation::None};
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }

  // get root pid first then search
  std::optional<ReadPageGuard> header_lk_read{bpm_->ReadPage(header_page_id_)};
  const BPlusTreeHeaderPage *header_node{header_lk_read->As<BPlusTreeHeaderPage>()};

  page_id_t current_pid{header_node->root_page_id_};

  std::optional<ReadPageGuard> leaf_read_lk{std::nullopt};
  std::optional<ReadPageGuard> curr_lk{std::nullopt};
  std::optional<ReadPageGuard> prev_lk{std::nullopt};

  while (true) {
    std::optional<ReadPageGuard> curr_lk{bpm_->ReadPage(current_pid)};
    if (header_lk_read.has_value()) header_lk_read.reset();

    const BPlusTreePage *node{curr_lk->As<BPlusTreePage>()};

    if (node->IsLeafPage()) {
      leaf_read_lk = std::move(curr_lk);
      break;
    }

    const InternalPage *internal_node{curr_lk->As<InternalPage>()};
    page_id_t next_page_id{internal_node->ValueAt(0)};

    current_pid = next_page_id;
    prev_lk = std::move(curr_lk);
  }
  prev_lk.reset();
  return INDEXITERATOR_TYPE{bpm_, current_pid, std::move(leaf_read_lk), 0};
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return End();
  }

  // get root pid first then search
  std::optional<ReadPageGuard> header_lk_read{bpm_->ReadPage(header_page_id_)};
  const BPlusTreeHeaderPage *header_node{header_lk_read->As<BPlusTreeHeaderPage>()};

  page_id_t current_pid{header_node->root_page_id_};

  std::optional<ReadPageGuard> leaf_read_lk{std::nullopt};
  std::optional<ReadPageGuard> curr_lk{std::nullopt};
  std::optional<ReadPageGuard> prev_lk{std::nullopt};

  while (true) {
    std::optional<ReadPageGuard> curr_lk{bpm_->ReadPage(current_pid)};
    if (header_lk_read.has_value()) header_lk_read.reset();

    const BPlusTreePage *node{curr_lk->As<BPlusTreePage>()};

    if (node->IsLeafPage()) {
      leaf_read_lk = std::move(curr_lk);
      break;
    }

    const InternalPage *internal_node{curr_lk->As<InternalPage>()};
    page_id_t next_page_id{internal_node->Lookup(key, comparator_)};

    current_pid = next_page_id;
    prev_lk = std::move(curr_lk.value());
  }
  prev_lk.reset();
  const LeafPage *leaf_node{leaf_read_lk->As<LeafPage>()};
  int leaf_index{leaf_node->KeyPos(key, comparator_)};
  return INDEXITERATOR_TYPE{bpm_, current_pid, std::move(leaf_read_lk), leaf_index};
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE{bpm_, INVALID_PAGE_ID, std::nullopt}; }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard header_gaurd{bpm_->ReadPage(header_page_id_)};

  const BPlusTreeHeaderPage *header_node{header_gaurd.As<BPlusTreeHeaderPage>()};

  return header_node->root_page_id_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeInternalPage(InternalPage *parent, InternalPage *node1, InternalPage *node2, int node1_idx,
                                       int node2_idx) {
  if (node1_idx < node2_idx) {
    KeyType key{parent->KeyAt(node2_idx)};
    node1->Merge(node2, key);
    parent->DeletePair(node2_idx);
  } else {
    KeyType key{parent->KeyAt(node1_idx)};
    node2->Merge(node1, key);
    parent->DeletePair(node1_idx);
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowInternalPage(InternalPage *parent, InternalPage *donor, InternalPage *receiver, int d_idx,
                                        int r_idx) {
  if (r_idx < d_idx) {
    page_id_t pid_to_r{donor->ValueAt(0)};
    KeyType key_to_r{parent->KeyAt(d_idx)};

    receiver->InsertElement(key_to_r, pid_to_r, comparator_);
    parent->SetKeyAt(d_idx, donor->KeyAt(1));

    // shift donor arrays by 1
    donor->SetValueAt(0, donor->ValueAt(1));

    for (int i = 2; i < donor->GetSize(); i++) {
      donor->SetKeyAt(i - 1, donor->KeyAt(i));
      donor->SetValueAt(i - 1, donor->ValueAt(i));
    }
    donor->ChangeSizeBy(-1);

  } else {
    page_id_t pid_to_r{donor->ValueAt(donor->GetSize() - 1)};
    KeyType key_to_r{parent->KeyAt(r_idx)};

    int current_size = receiver->GetSize();
    for (int i = current_size; i > 0; i--) {
      if (i > 1) {
        receiver->SetKeyAt(i, receiver->KeyAt(i - 1));
      }
      receiver->SetValueAt(i, receiver->ValueAt(i - 1));
    }
    receiver->SetValueAt(1, receiver->ValueAt(0));

    receiver->SetKeyAt(1, key_to_r);
    receiver->SetValueAt(0, pid_to_r);
    receiver->ChangeSizeBy(1);

    parent->SetKeyAt(r_idx, donor->KeyAt(donor->GetSize() - 1));

    donor->ChangeSizeBy(-1);
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowLeafPage(InternalPage *parent, LeafPage *donor, LeafPage *receiver, int d_idx, int r_idx) {
  if (r_idx < d_idx) {
    KeyType key{donor->KeyAt(0)};
    ValueType value{donor->ValueAt(0)};

    receiver->InsertElement(key, value, comparator_);
    donor->DeletePair(0);
    parent->SetKeyAt(d_idx, donor->KeyAt(0));

  } else {
    int size{donor->GetSize()};
    KeyType key{donor->KeyAt(size - 1)};
    ValueType value{donor->ValueAt(size - 1)};

    receiver->InsertElement(key, value, comparator_);
    donor->ChangeSizeBy(-1);
    parent->SetKeyAt(r_idx, receiver->KeyAt(0));
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeLeafPage(InternalPage *parent, LeafPage *node1, LeafPage *node2, int node1_idx,
                                   int node2_idx) {
  if (node1_idx < node2_idx) {
    node1->Merge(node2);
    parent->DeletePair(node2_idx);
  } else {
    node2->Merge(node1);
    parent->DeletePair(node1_idx);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
