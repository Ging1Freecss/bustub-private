//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.cpp
//
// Identification: src/execution/window_function_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/window_function_executor.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <vector>
#include "binder/bound_order_by.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/external_merge_sort_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "fmt/base.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new WindowFunctionExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The window aggregation plan to be executed
 */
WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the window aggregation */
void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  tuples_per_window.clear();

  output_tuples.Clear();
  input_tuples.Clear();
  enriched_schema = std::nullopt;
  results.clear();
  is_finished = false;
  total_tuples = 0;

  input_tuples = TupleDiskStorage{{}, exec_ctx_->GetBufferPoolManager(), 0};
  output_tuples = TupleDiskStorage{{}, exec_ctx_->GetBufferPoolManager(), 0};

  for (auto &[col, wf] : plan_->window_functions_) {
    if (!wf.order_by_.empty()) {
      output_order = wf.order_by_;
    }
  }

  initialise_input();

  for (auto &[col, wf] : plan_->window_functions_) {
    process_window_functions(col);
  }

  input_iter_ = input_tuples.Begin();

  window_iters_.clear();
  for (auto &[col_idx, tuple_data] : calc_output) {
    // std::get<0>(tuple_data) gets the TupleDiskStorage
    window_iters_.insert({col_idx, std::get<0>(tuple_data).Begin()});
  }
}

void WindowFunctionExecutor::initialise_input() {
  std::vector<Tuple> child_tuple{};
  std::vector<RID> child_rid{};

  BufferPoolManager *bpm_{exec_ctx_->GetBufferPoolManager()};

  TupleDiskStorage temp_storage{std::vector<page_id_t>{}, bpm_, 0};

  page_id_t new_page_id{bpm_->NewPage()};
  std::optional<WritePageGuard> page_gaurd{bpm_->CheckedWritePage(new_page_id)};
  IntermediateResultPage *page_ptr{page_gaurd->AsMut<IntermediateResultPage>()};
  page_ptr->Init();

  auto child_cols = child_executor_->GetOutputSchema().GetColumns();
  child_cols.emplace_back("__row_idx", TypeId::INTEGER);
  enriched_schema = Schema{child_cols};

  uint32_t row_idx = 0;
  while (child_executor_->Next(&child_tuple, &child_rid, BUSTUB_BATCH_SIZE)) {
    for (size_t i{0}; i < child_tuple.size();) {
      if (!page_ptr->InsertTuple(child_tuple[i])) {
        // loop over each window function and create a separate page save for each
        uint32_t max_num_tp{page_ptr->GetNumTuples()};

        if (page_ptr->GetNumTuples() == 0) {
          throw bustub::ExecutionException("Tuple too large to fit in a single IntermediateResultPage");
        }
        for (uint32_t a{0}; a < max_num_tp; a++) {
          Tuple tp{page_ptr->GetTuple(a)};
          temp_storage.InsertTuple(tp);
        }

        page_ptr->Init();
      } else {
        i++;
        row_idx++;
      }
    }
  }

  total_tuples = row_idx;

  if (page_ptr->GetNumTuples() > 0) {
    uint32_t max_num_tp{page_ptr->GetNumTuples()};
    for (uint32_t a{0}; a < max_num_tp; a++) {
      Tuple tp{page_ptr->GetTuple(a)};
      temp_storage.InsertTuple(tp);
    }
    page_ptr->Init();
  }

  // output order if not empty sort the tuple based on order by
  if (!output_order.empty()) {
    TupleComparator cmp{output_order};
    Schema child_tuple_schema{child_executor_->GetOutputSchema()};
    TupleDiskStorage temp_sorted_storage =
        standard_tuple_disk_storage_sort(temp_storage, cmp, output_order, child_tuple_schema);
    temp_storage.Clear();
    temp_storage = std::move(temp_sorted_storage);
  }

  // reset it for index of tuple
  row_idx = 0;

  for (auto tuple_itr{temp_storage.Begin()}; tuple_itr != temp_storage.End();) {
    Tuple tp_input{*tuple_itr};

    // Build enriched tuple: original values + row index
    std::vector<Value> vals;
    for (uint32_t c = 0; c < child_executor_->GetOutputSchema().GetColumnCount(); c++) {
      vals.push_back(tp_input.GetValue(&child_executor_->GetOutputSchema(), c));
    }

    vals.push_back(ValueFactory::GetIntegerValue(row_idx));
    Tuple enriched{vals, &enriched_schema.value()};

    if (!page_ptr->InsertTuple(enriched)) {
      uint32_t max_num_tp{page_ptr->GetNumTuples()};

      for (uint32_t a{0}; a < max_num_tp; a++) {
        Tuple tp{page_ptr->GetTuple(a)};
        input_tuples.InsertTuple(tp);
      }

      page_ptr->Init();
    } else {
      ++tuple_itr;
      ++row_idx;
    }
  }

  // insert left over tuples

  if (page_ptr->GetNumTuples() > 0) {
    uint32_t max_num_tp{page_ptr->GetNumTuples()};

    for (uint32_t a{0}; a < max_num_tp; a++) {
      Tuple tp{page_ptr->GetTuple(a)};
      input_tuples.InsertTuple(tp);
    }

    page_ptr->Init();
  }

  // Delete the original array-packing buffer page!
  temp_storage.Clear();
  page_gaurd.reset();
  bpm_->DeletePage(new_page_id);
}

auto WindowFunctionExecutor::process_window_functions(uint32_t wind_fn_idx) -> void {
  // If the child executor returned exactly 0 tuples, we do absolutely nothing!
  if (input_tuples.Begin() == input_tuples.End()) {
    return;
  }

  BufferPoolManager *bpm_{exec_ctx_->GetBufferPoolManager()};

  // for intermediate page , buffer from where we insert tuple to temp_sort_storage
  page_id_t new_page_id{bpm_->NewPage()};
  std::optional<WritePageGuard> page_gaurd{bpm_->CheckedWritePage(new_page_id)};
  IntermediateResultPage *page_ptr{page_gaurd->AsMut<IntermediateResultPage>()};
  page_ptr->Init();

  // temp storage for sort function
  /*
    temp_sort_storage has this schema
    |partition keys|order by keys|output window function|row number
  */
  std::vector<TupleDiskStorage> temp_sort_storage;
  std::optional<Schema> window_tuple_schema{std::nullopt};
  std::vector<OrderBy> combine_keys{};  // keys for comparator fn

  // compartor function for this window function
  std::optional<TupleComparator> cmp;
  std::vector<SortEntry> buffer_{};

  // function to generate new page for temp_sort_storage
  auto generateNewPage{[&bpm_](std::optional<WritePageGuard> &page_gaurd) {
    page_id_t new_page_id{bpm_->NewPage()};

    page_gaurd = bpm_->CheckedWritePage(new_page_id);
    if (!page_gaurd.has_value()) {
      throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
    }
  }};

  if (auto it{plan_->window_functions_.find(wind_fn_idx)}; it != plan_->window_functions_.end()) {
    // combine keys for sorting
    uint32_t current_idx = 0;
    for (const AbstractExpressionRef &p : it->second.partition_by_) {
      // has to do this as original ColumnValueExpression has different column index and we are inserting modified tuple
      auto col_expr = std::make_shared<ColumnValueExpression>(0, current_idx, p->GetReturnType());
      combine_keys.push_back(OrderBy{OrderByType::ASC, OrderByNullType::DEFAULT, col_expr});
      current_idx++;
    }

    for (const auto &[order_type, order_null_type, expr] : it->second.order_by_) {
      // has to do this as original ColumnValueExpression has different column index and we are inserting modified tuple
      auto col_expr = std::make_shared<ColumnValueExpression>(0, current_idx, expr->GetReturnType());
      combine_keys.push_back(OrderBy{order_type, order_null_type, col_expr});
      current_idx++;
    }

    // compartor function for this window function
    cmp = TupleComparator{combine_keys};

    // end of combine keys

    // columns
    std::vector<Column> output_colms;

    // push the column of partition by first important for sorting algo
    for (auto &partition_ele : it->second.partition_by_) {
      output_colms.push_back(partition_ele->GetReturnType());
    }

    // column of order by clause
    for (auto &[order_type, order_null_type, expr] : it->second.order_by_) {
      output_colms.push_back(expr->GetReturnType());
    }

    // column of return value of window function
    output_colms.push_back(it->second.function_->GetReturnType());

    // column of index inserted in input tuple
    output_colms.push_back(enriched_schema->GetColumn(enriched_schema->GetColumnCount() - 1));

    window_tuple_schema = Schema{output_colms};

    /*
      main logic is that create tuple insert into page_ptr till it full means that the
      max amount of tuple of window_tuple_schema can be hold by page_ptr then traverse
      through it then insert into buffer sort it then insert into page_ptr_sort which is
      push into temp_sort_storage
    */
    for (auto tuple_itr{input_tuples.Begin()}; tuple_itr != input_tuples.End();) {
      Tuple tp{*tuple_itr};

      std::vector<Value> output_vals;

      for (auto &partition_ele : it->second.partition_by_) {
        output_vals.push_back(partition_ele->Evaluate(&tp, child_executor_->GetOutputSchema()));
      }

      for (auto &[order_type, order_null_type, expr] : it->second.order_by_) {
        output_vals.push_back(expr->Evaluate(&tp, child_executor_->GetOutputSchema()));
      }

      output_vals.push_back(it->second.function_->Evaluate(&tp, child_executor_->GetOutputSchema()));

      output_vals.push_back(tp.GetValue(&enriched_schema.value(), enriched_schema->GetColumnCount() - 1));

      Tuple output_tuple{output_vals, &window_tuple_schema.value()};

      if (!page_ptr->InsertTuple(output_tuple)) {
        uint32_t max_num_tp{page_ptr->GetNumTuples()};

        for (uint32_t a{0}; a < max_num_tp; a++) {
          Tuple tp{page_ptr->GetTuple(a)};
          SortKey sort_key{GenerateSortKey(tp, combine_keys, window_tuple_schema.value())};
          buffer_.push_back({std::move(sort_key), tp});
        }

        std::sort(buffer_.begin(), buffer_.end(), cmp.value());

        std::optional<WritePageGuard> page_gaurd_sort{};
        generateNewPage(page_gaurd_sort);

        page_id_t pid{page_gaurd_sort->GetPageId()};

        IntermediateResultPage *page_ptr_sort{page_gaurd_sort->AsMut<IntermediateResultPage>()};
        page_ptr_sort->Init();

        for (size_t a{0}; a < buffer_.size(); a++) {
          if (!page_ptr_sort->InsertTuple(buffer_[a].second)) {
            throw bustub::Exception("failed in inserting at page_ptr_sort");
          }
        }

        temp_sort_storage.emplace_back(std::vector{pid}, bpm_, buffer_.size());

        // reset and clear buffers
        buffer_.clear();
        page_ptr->Init();
      } else {
        ++tuple_itr;
      }
    }

    // insert left over tuples

    if (page_ptr->GetNumTuples() > 0) {
      uint32_t max_num_tp{page_ptr->GetNumTuples()};

      for (uint32_t a{0}; a < max_num_tp; a++) {
        Tuple tp{page_ptr->GetTuple(a)};
        SortKey sort_key{GenerateSortKey(tp, combine_keys, window_tuple_schema.value())};
        buffer_.push_back({std::move(sort_key), tp});
      }

      std::sort(buffer_.begin(), buffer_.end(), cmp.value());

      std::optional<WritePageGuard> page_gaurd_sort{};
      generateNewPage(page_gaurd_sort);
      page_id_t pid{page_gaurd_sort->GetPageId()};
      IntermediateResultPage *page_ptr_sort{page_gaurd_sort->AsMut<IntermediateResultPage>()};
      page_ptr_sort->Init();

      for (size_t a{0}; a < buffer_.size(); a++) {
        if (!page_ptr_sort->InsertTuple(buffer_[a].second)) {
          throw bustub::Exception("failed in inserting at page_ptr_sort");
        }
      }

      temp_sort_storage.emplace_back(std::vector{pid}, bpm_, buffer_.size());

      buffer_.clear();
    }

    // sort the pages
    TupleDiskStorage sorted_pages{
        do_mergePass(temp_sort_storage, cmp.value(), combine_keys, window_tuple_schema.value())};

    // return output of window fn unsorted by index
    auto [calc_tuple_disk, calc_order_by, calc_schema] =
        calc_window_function(sorted_pages, cmp.value(), combine_keys, window_tuple_schema.value(), it->second);
    TupleComparator calc_cmp{calc_order_by};

    // sort the output of calc of window function
    TupleDiskStorage sorted_window_calc =
        standard_tuple_disk_storage_sort(calc_tuple_disk, calc_cmp, calc_order_by, calc_schema);

    // insert sorted page with index|window function output , with schema and order by vector
    calc_output.emplace(wind_fn_idx, std::tuple{sorted_window_calc, calc_schema, calc_order_by});

    // clean up
    calc_tuple_disk.Clear();
    sorted_pages.Clear();

    // Delete the original array-packing buffer page!
    page_gaurd.reset();
    bpm_->DeletePage(new_page_id);
  } else {
    throw bustub::Exception(std::to_string(wind_fn_idx) + ": this col does not exist in plan_->window_functions_");
  }
}

auto WindowFunctionExecutor::do_mergePass(std::vector<TupleDiskStorage> &runs, TupleComparator &cmp,
                                          std::vector<OrderBy> &combine_keys, Schema &tuple_schema)
    -> TupleDiskStorage {
  TupleDiskStorage output_storage{std::vector<page_id_t>{}, exec_ctx_->GetBufferPoolManager(), 0};
  // helper function to merge TupleDiskStorage
  auto MergeTwo{[&](TupleDiskStorage &m1, TupleDiskStorage &m2, TupleDiskStorage &temp_buf) {
    {
      TupleDiskStorage::Iterator a = m1.Begin();
      TupleDiskStorage::Iterator b = m2.Begin();
      if (a == m1.End() && b == m2.End()) {
        return;
      }

      SortKey key_a, key_b;
      bool has_a = false;
      bool has_b = false;

      if (a != m1.End()) {
        key_a = GenerateSortKey(*a, combine_keys, tuple_schema);
        has_a = true;
      }
      if (b != m2.End()) {
        key_b = GenerateSortKey(*b, combine_keys, tuple_schema);
        has_b = true;
      }

      while (has_a && has_b) {
        SortEntry entry_a{key_a, *a};
        SortEntry entry_b{key_b, *b};
        if (cmp(entry_a, entry_b)) {
          temp_buf.InsertTuple(*a);
          ++a;
          if (a != m1.End()) {
            key_a = GenerateSortKey(*a, combine_keys, tuple_schema);
          } else {
            has_a = false;
          }
        } else {
          temp_buf.InsertTuple(*b);
          ++b;
          if (b != m2.End()) {
            key_b = GenerateSortKey(*b, combine_keys, tuple_schema);
          } else {
            has_b = false;
          }
        }
      }
      while (a != m1.End()) {
        temp_buf.InsertTuple(*a);
        ++a;
      }
      while (b != m2.End()) {
        temp_buf.InsertTuple(*b);
        ++b;
      }
    }
  }};

  /*
    main logic is to merge n ele of vector runs to (n/2) element
    keep on doing it till size = 1
  */

  while (runs.size() > 1) {
    std::vector<TupleDiskStorage> new_runs{};
    for (size_t i{0}; i < runs.size(); i += 2) {
      if (i + 1 >= runs.size()) {
        // Odd run, pass through
        new_runs.push_back(std::move(runs[i]));
        break;
      }
      TupleDiskStorage temp_buf{{}, exec_ctx_->GetBufferPoolManager(), 0};

      MergeTwo(runs[i], runs[i + 1], temp_buf);

      // clear buffer
      runs[i].Clear();
      runs[i + 1].Clear();

      // for next iteration
      new_runs.push_back(std::move(temp_buf));
    }
    runs = std::move(new_runs);
  }

  if (runs.size() == 1) {
    output_storage = std::move(runs[0]);
  } else {
    throw bustub::Exception("in do_mergePass runs did not have size=1 after merging");
  }

  return output_storage;
}

auto WindowFunctionExecutor::calc_window_function(TupleDiskStorage &input_for_wind, TupleComparator &cmp,
                                                  std::vector<OrderBy> &combine_keys, Schema &tuple_schema,
                                                  const WindowFunctionPlanNode::WindowFunction &wf)
    -> std::tuple<TupleDiskStorage, std::vector<OrderBy>, Schema> {
  auto WindowInitialValue{[](WindowFunctionType wf) {
    switch (wf) {
      case bustub::WindowFunctionType::CountStarAggregate:
        return ValueFactory::GetIntegerValue(0);
        break;
      case bustub::WindowFunctionType::CountAggregate:
      case bustub::WindowFunctionType::MaxAggregate:
      case bustub::WindowFunctionType::MinAggregate:
      case bustub::WindowFunctionType::SumAggregate:
        return ValueFactory::GetNullValueByType(TypeId::INTEGER);
      case bustub::WindowFunctionType::Rank:
        return ValueFactory::GetIntegerValue(0);
    }
    UNREACHABLE("Unknown window function type");
  }};

  auto WindowCombineAgg{[](Value &result, Value &input, WindowFunctionType wf) {
    switch (wf) {
      case bustub::WindowFunctionType::CountStarAggregate:
        result = result.Add(ValueFactory::GetIntegerValue(1));
        return;
      case bustub::WindowFunctionType::CountAggregate:

        if (!input.IsNull()) {
          if (result.IsNull()) {
            result = ValueFactory::GetIntegerValue(1);
          } else {
            result = result.Add(ValueFactory::GetIntegerValue(1));
          }
        }
        return;

      case bustub::WindowFunctionType::MaxAggregate:
        if (!input.IsNull()) {
          if (result.IsNull()) {
            result = input;
          } else {
            result = result.Max(input);
          }
        }
        return;
      case bustub::WindowFunctionType::MinAggregate:
        if (!input.IsNull()) {
          if (result.IsNull()) {
            result = input;
          } else {
            result = result.Min(input);
          }
        }
        return;

      case bustub::WindowFunctionType::SumAggregate:
        if (!input.IsNull()) {
          if (result.IsNull()) {
            result = input;
          } else {
            result = result.Add(input);
          }
        }
        return;
      case bustub::WindowFunctionType::Rank:
        result = ValueFactory::GetIntegerValue(0);
        return;
    }
  }};
  auto VectorValuesEqual = [](const std::vector<Value> &a, const std::vector<Value> &b) -> bool {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); i++) {
      if (!a[i].CompareExactlyEquals(b[i])) return false;
    }
    return true;
  };

  int64_t row_num_idx{static_cast<int64_t>(tuple_schema.GetColumnCount()) - 1};
  int64_t input_idx{static_cast<int64_t>(tuple_schema.GetColumnCount()) - 2};

  // unsorted output for window calculation , later sorted by row number
  TupleDiskStorage unsorted_win_calc{std::vector<page_id_t>{}, exec_ctx_->GetBufferPoolManager(), 0};
  std::vector<Column> final_cols;
  final_cols.emplace_back("__row_idx", TypeId::INTEGER);
  final_cols.emplace_back("window_result", wf.function_->GetReturnType());
  Schema final_schema{final_cols};

  auto col_expr = std::make_shared<ColumnValueExpression>(0, 0, tuple_schema.GetColumn(row_num_idx));
  std::vector<OrderBy> unsorted_order_by{OrderBy{OrderByType::ASC, OrderByNullType::DEFAULT, col_expr}};

  int64_t position{0};
  int64_t rank{0};

  std::vector<Value> prev_partition;
  std::vector<Value> prev_order_by;

  Value result{WindowInitialValue(wf.type_)};

  std::vector<Value> partition_rows;
  bool first_row = true;

  for (TupleDiskStorage::Iterator it{input_for_wind.Begin()}; it != input_for_wind.End(); ++it) {
    const Tuple &tp{*it};
    Value org_idx{tp.GetValue(&tuple_schema, row_num_idx)};

    std::vector<Value> curr_partition;
    for (size_t i{0}; i < wf.partition_by_.size(); i++) {
      curr_partition.push_back(std::get<AbstractExpressionRef>(combine_keys[i])->Evaluate(&tp, tuple_schema));
    }

    std::vector<Value> curr_order{};
    for (size_t i{0}; i < wf.order_by_.size(); i++) {
      size_t idx{wf.partition_by_.size() + i};
      curr_order.push_back(std::get<AbstractExpressionRef>(combine_keys[idx])->Evaluate(&tp, tuple_schema));
    }

    Value input{tp.GetValue(&tuple_schema, input_idx)};

    // check for partition change
    bool new_partition{first_row || !VectorValuesEqual(prev_partition, curr_partition)};
    bool new_order = first_row || !VectorValuesEqual(prev_order_by, curr_order);
    bool new_group = new_partition || new_order;
    first_row = false;

    if (wf.order_by_.empty()) {
      position++;

      if (new_partition && !partition_rows.empty()) {
        for (Value row : partition_rows) {
          std::vector<Value> result_vals;
          result_vals.push_back(row);
          result_vals.push_back(result);
          Tuple final_tuple{result_vals, &final_schema};
          unsorted_win_calc.InsertTuple(final_tuple);
        }

        partition_rows.clear();
        result = WindowInitialValue(wf.type_);
      }

      // push current row idx and combine result
      partition_rows.push_back(org_idx);
      WindowCombineAgg(result, input, wf.type_);
    } else {
      if (new_group) {
        // 1. Flush the PREVIOUS group
        if (!partition_rows.empty()) {
          for (Value row : partition_rows) {
            std::vector<Value> result_vals;
            result_vals.push_back(row);

            // Fix: Push rank instead of result if it's a Rank function!
            if (wf.type_ == bustub::WindowFunctionType::Rank) {
              result_vals.push_back(ValueFactory::GetIntegerValue(rank));
            } else {
              result_vals.push_back(result);
            }

            Tuple final_tuple{result_vals, &final_schema};
            unsorted_win_calc.InsertTuple(final_tuple);
          }
          partition_rows.clear();
        }
        if (new_partition) {
          // 2. Reset partition-specific state
          position = 0;
          rank = 0;
          result = WindowInitialValue(wf.type_);
          prev_order_by.clear();  // Force the next row to be a new group
        }
      }

      position++;
      if (new_group) {
        rank = position;  // Correctly sets rank=1 for the first row
      }
      WindowCombineAgg(result, input, wf.type_);

      partition_rows.push_back(org_idx);
    }

    // update prev partition safe to move as no need for current partition
    prev_order_by = std::move(curr_order);
    prev_partition = std::move(curr_partition);
  }

  if (wf.order_by_.empty() && !partition_rows.empty()) {
    for (Value row : partition_rows) {
      std::vector<Value> result_vals;
      result_vals.push_back(row);
      result_vals.push_back(result);
      Tuple final_tuple{result_vals, &final_schema};
      unsorted_win_calc.InsertTuple(final_tuple);
    }
  }

  if (!wf.order_by_.empty() && !partition_rows.empty()) {
    for (Value row : partition_rows) {
      std::vector<Value> result_vals;
      result_vals.push_back(row);

      // Fix: Push rank instead of result if it's a Rank function!
      if (wf.type_ == bustub::WindowFunctionType::Rank) {
        result_vals.push_back(ValueFactory::GetIntegerValue(rank));
      } else {
        result_vals.push_back(result);
      }

      Tuple final_tuple{result_vals, &final_schema};
      unsorted_win_calc.InsertTuple(final_tuple);
    }
  }

  return std::tuple{unsorted_win_calc, unsorted_order_by, final_schema};
}

auto WindowFunctionExecutor::standard_tuple_disk_storage_sort(TupleDiskStorage &page_input, TupleComparator &cmp,
                                                              std::vector<OrderBy> &combine_keys, Schema &tuple_schema)
    -> TupleDiskStorage {
  std::vector<SortEntry> buffer_{};
  std::vector<TupleDiskStorage> temp_sort_storage;

  BufferPoolManager *bpm_{exec_ctx_->GetBufferPoolManager()};

  page_id_t new_page_id{bpm_->NewPage()};
  std::optional<WritePageGuard> page_gaurd{bpm_->CheckedWritePage(new_page_id)};
  IntermediateResultPage *page_ptr{page_gaurd->AsMut<IntermediateResultPage>()};
  page_ptr->Init();

  // function to generate new page for temp_sort_storage
  auto generateNewPage{[&bpm_](std::optional<WritePageGuard> &page_gaurd) {
    page_id_t new_page_id{bpm_->NewPage()};

    page_gaurd = bpm_->CheckedWritePage(new_page_id);
    if (!page_gaurd.has_value()) {
      throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
    }
  }};

  for (auto tuple_itr{page_input.Begin()}; tuple_itr != page_input.End();) {
    Tuple tp{*tuple_itr};
    if (!page_ptr->InsertTuple(tp)) {
      uint32_t max_num_tp{page_ptr->GetNumTuples()};

      for (uint32_t a{0}; a < max_num_tp; a++) {
        Tuple tp{page_ptr->GetTuple(a)};
        SortKey sort_key{GenerateSortKey(tp, combine_keys, tuple_schema)};
        buffer_.push_back({std::move(sort_key), tp});
      }

      std::sort(buffer_.begin(), buffer_.end(), cmp);

      std::optional<WritePageGuard> page_gaurd_sort{};
      generateNewPage(page_gaurd_sort);

      page_id_t pid{page_gaurd_sort->GetPageId()};

      IntermediateResultPage *page_ptr_sort{page_gaurd_sort->AsMut<IntermediateResultPage>()};
      page_ptr_sort->Init();

      for (size_t a{0}; a < buffer_.size(); a++) {
        if (!page_ptr_sort->InsertTuple(buffer_[a].second)) {
          throw bustub::Exception("failed in inserting at page_ptr_sort");
        }
      }

      temp_sort_storage.emplace_back(std::vector{pid}, bpm_, buffer_.size());

      // reset and clear buffers
      buffer_.clear();
      page_ptr->Init();
    } else {
      ++tuple_itr;
    }
  }

  if (page_ptr->GetNumTuples() > 0) {
    uint32_t max_num_tp{page_ptr->GetNumTuples()};

    for (uint32_t a{0}; a < max_num_tp; a++) {
      Tuple tp{page_ptr->GetTuple(a)};
      SortKey sort_key{GenerateSortKey(tp, combine_keys, tuple_schema)};
      buffer_.push_back({std::move(sort_key), tp});
    }

    std::sort(buffer_.begin(), buffer_.end(), cmp);

    std::optional<WritePageGuard> page_gaurd_sort{};
    generateNewPage(page_gaurd_sort);

    page_id_t pid{page_gaurd_sort->GetPageId()};

    IntermediateResultPage *page_ptr_sort{page_gaurd_sort->AsMut<IntermediateResultPage>()};
    page_ptr_sort->Init();

    for (size_t a{0}; a < buffer_.size(); a++) {
      if (!page_ptr_sort->InsertTuple(buffer_[a].second)) {
        throw bustub::Exception("failed in inserting at page_ptr_sort");
      }
    }

    temp_sort_storage.emplace_back(std::vector{pid}, bpm_, buffer_.size());

    // reset and clear buffers
    buffer_.clear();
    page_ptr->Init();
  }

  // Delete the original array-packing buffer page!
  page_gaurd.reset();
  bpm_->DeletePage(new_page_id);

  return do_mergePass(temp_sort_storage, cmp, combine_keys, tuple_schema);
}

/**
 * Yield the next tuple batch from the window aggregation.
 * @param[out] tuple_batch The next tuple batch produced by the window aggregation
 * @param[out] rid_batch The next tuple RID batch produced by the window aggregation
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto WindowFunctionExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  if (is_finished) return false;
  // Keep building tuples until the batch is full or we run out of input
  while (input_iter_ != input_tuples.End() && tuple_batch->size() < batch_size) {
    Tuple input_tp = *input_iter_;
    std::vector<Value> output_vals;
    // Loop over every column requested by the output schema
    for (uint32_t col = 0; col < plan_->columns_.size(); ++col) {
      if (plan_->window_functions_.count(col)) {
        // Option A: This column is a Window Function!

        // 1. Get the iterator for this specific window function
        auto &win_iter = window_iters_.at(col);

        // 2. Read the tiny tuple [__row_idx, result]
        Tuple win_tp = *win_iter;

        // 3. Extract the result (column index 1 in final_schema)
        auto &tiny_schema = std::get<1>(calc_output.at(col));
        Value result = win_tp.GetValue(&tiny_schema, 1);

        // 4. Push it to the final output
        output_vals.push_back(result);

        // 5. Advance the iterator for the next row!
        ++win_iter;

      } else {
        // Option B: This is a normal column
        // Just extract it directly from the original input tuple!
        output_vals.push_back(plan_->columns_[col]->Evaluate(&input_tp, enriched_schema.value()));
      }
    }
    // Combine the values into the final output tuple!
    Tuple output{output_vals, &GetOutputSchema()};
    tuple_batch->push_back(output);
    rid_batch->push_back(RID{});

    // Advance the input sequence iterator
    ++input_iter_;
  }
  is_finished = tuple_batch->empty();
  // If we just exhausted the last data row, clean everything up immediately!
  if (is_finished) {
    input_tuples.Clear();
    for (auto &[col, data] : calc_output) {
      std::get<0>(data).Clear();
    }
  }

  return !is_finished;  // Returns true if we produced a batch
}
}  // namespace bustub
