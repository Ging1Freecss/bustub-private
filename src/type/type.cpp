//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// type.cpp
//
// Identification: src/type/type.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "type/type.h"
#include <cstdint>
#include <string>
#include "common/exception.h"
#include "type/bigint_type.h"
#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/integer_type.h"
#include "type/smallint_type.h"
#include "type/timestamp_type.h"
#include "type/tinyint_type.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/varlen_type.h"
#include "type/vector_type.h"

namespace bustub {

Type *Type::k_types[] = {new Type(TypeId::INVALID),
                         new BooleanType(),
                         new TinyintType(),
                         new SmallintType(),
                         new IntegerType(TypeId::INTEGER),
                         new BigintType(),
                         new DecimalType(),
                         new VarlenType(TypeId::VARCHAR),
                         new TimestampType(),
                         new VectorType()};

/**
 * Get the size of this data type in bytes
 */
auto Type::GetTypeSize(const TypeId type_id) -> uint64_t {
  switch (type_id) {
    case BOOLEAN:
    case TINYINT:
      return 1;
    case SMALLINT:
      return 2;
    case INTEGER:
      return 4;
    case BIGINT:
    case DECIMAL:
    case TIMESTAMP:
      return 8;
    case VARCHAR:
      return 0;
    default:
      break;
  }
  throw Exception(ExceptionType::UNKNOWN_TYPE, "Unknown type.");
}

/**
 * Is this type coercable from the other type
 */
auto Type::IsCoercableFrom(const TypeId type_id) const -> bool {
  switch (type_id_) {
    case INVALID:
      return false;
    case BOOLEAN:
      return true;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case DECIMAL:
      switch (type_id) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case DECIMAL:
        case VARCHAR:
          return true;
        default:
          return false;
      }
      break;
    case TIMESTAMP:
      return (type_id == VARCHAR || type_id == TIMESTAMP);
    case VARCHAR:
      switch (type_id) {
        case BOOLEAN:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case DECIMAL:
        case TIMESTAMP:
        case VARCHAR:
          return true;
        default:
          return false;
      }
      break;
    default:
      return (type_id == type_id_);
  }  // END SWITCH
}

/**
 * Debug info
 */
auto Type::TypeIdToString(const TypeId type_id) -> std::string {
  switch (type_id) {
    case INVALID:
      return "INVALID";
    case BOOLEAN:
      return "BOOLEAN";
    case TINYINT:
      return "TINYINT";
    case SMALLINT:
      return "SMALLINT";
    case INTEGER:
      return "INTEGER";
    case BIGINT:
      return "BIGINT";
    case DECIMAL:
      return "DECIMAL";
    case TIMESTAMP:
      return "TIMESTAMP";
    case VARCHAR:
      return "VARCHAR";
    case VECTOR:
      return "VECTOR";
    default:
      return "INVALID";
  }
}

auto Type::GetMinValue(TypeId type_id) -> Value {
  switch (type_id) {
    case BOOLEAN:
      return {type_id, 0};
    case TINYINT:
      return {type_id, BUSTUB_INT8_MIN};
    case SMALLINT:
      return {type_id, BUSTUB_INT16_MIN};
    case INTEGER:
      return {type_id, BUSTUB_INT32_MIN};
    case BIGINT:
      return {type_id, BUSTUB_INT64_MIN};
    case DECIMAL:
      return {type_id, BUSTUB_DECIMAL_MIN};
    case TIMESTAMP:
      return {type_id, 0};
    case VARCHAR:
      return {type_id, ""};
    default:
      break;
  }
  throw Exception(ExceptionType::MISMATCH_TYPE, "Cannot get minimal value.");
}

auto Type::GetMaxValue(TypeId type_id) -> Value {
  switch (type_id) {
    case BOOLEAN:
      return {type_id, 1};
    case TINYINT:
      return {type_id, BUSTUB_INT8_MAX};
    case SMALLINT:
      return {type_id, BUSTUB_INT16_MAX};
    case INTEGER:
      return {type_id, BUSTUB_INT32_MAX};
    case BIGINT:
      return {type_id, BUSTUB_INT64_MAX};
    case DECIMAL:
      return {type_id, BUSTUB_DECIMAL_MAX};
    case TIMESTAMP:
      return {type_id, BUSTUB_TIMESTAMP_MAX};
    case VARCHAR:
      return {type_id, nullptr, 0, false};
    default:
      break;
  }
  throw Exception(ExceptionType::MISMATCH_TYPE, "Cannot get max value.");
}

auto Type::CompareEquals(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
    -> CmpBool {
  if (left.type_id_ != right.type_id_) {
    return CmpBool::CmpFalse;
  }

  switch (left.type_id_) {
    case BOOLEAN:
      return (left.value_.boolean_ == right.value_.boolean_) ? CmpBool::CmpTrue : CmpBool::CmpFalse;
    case TINYINT:
      return (left.value_.tinyint_ == right.value_.tinyint_) ? CmpBool::CmpTrue : CmpBool::CmpFalse;
    case SMALLINT:
      return (left.value_.smallint_ == right.value_.smallint_) ? CmpBool::CmpTrue : CmpBool::CmpFalse;
    case INTEGER:
      return (left.value_.integer_ == right.value_.integer_) ? CmpBool::CmpTrue : CmpBool::CmpFalse;
    case BIGINT:
      return (left.value_.bigint_ == right.value_.bigint_) ? CmpBool::CmpTrue : CmpBool::CmpFalse;
    case DECIMAL:
      return (left.value_.decimal_ == right.value_.decimal_) ? CmpBool::CmpTrue : CmpBool::CmpFalse;
    case TIMESTAMP:
      return (left.value_.timestamp_ == right.value_.timestamp_) ? CmpBool::CmpTrue : CmpBool::CmpFalse;
    default:
      break;
  }
  throw Exception(ExceptionType::MISMATCH_TYPE, "Cannot compare value.");
}

auto Type::CompareNotEquals(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
    -> CmpBool {
  throw NotImplementedException("CompareNotEquals not implemented");
}

auto Type::CompareLessThan(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
    -> CmpBool {
  throw NotImplementedException("CompareLessThan not implemented");
}
auto Type::CompareLessThanEquals(const Value &left __attribute__((unused)),
                                 const Value &right __attribute__((unused))) const -> CmpBool {
  throw NotImplementedException("CompareLessThanEqual not implemented");
}
auto Type::CompareGreaterThan(const Value &left __attribute__((unused)),
                              const Value &right __attribute__((unused))) const -> CmpBool {
  throw NotImplementedException("CompareGreaterThan not implemented");
}
auto Type::CompareGreaterThanEquals(const Value &left __attribute__((unused)),
                                    const Value &right __attribute__((unused))) const -> CmpBool {
  throw NotImplementedException("CompareGreaterThanEqual not implemented");
}

// Other mathematical functions
auto Type::Add(const Value &left, const Value &right) const -> Value {
  if (left.type_id_ != right.type_id_) {
    throw Exception(ExceptionType::MISMATCH_TYPE, "type mismatch");
  }
  switch (left.type_id_) {
    case TINYINT:
      return Value(left.type_id_, static_cast<int8_t>(left.value_.tinyint_ + right.value_.tinyint_));
    case SMALLINT:
      return Value(left.type_id_, static_cast<int16_t>(left.value_.smallint_ + right.value_.smallint_));
    case INTEGER:
      return Value(left.type_id_, static_cast<int32_t>(left.value_.integer_ + right.value_.integer_));
    case BIGINT:
      return Value(left.type_id_, static_cast<int64_t>(left.value_.bigint_ + right.value_.bigint_));
    case DECIMAL:
      return Value(left.type_id_, static_cast<double>(left.value_.decimal_ + right.value_.decimal_));
    case TIMESTAMP:
      return Value(left.type_id_, static_cast<uint64_t>(left.value_.timestamp_ + right.value_.timestamp_));
    default:
      break;
  }
  throw Exception(ExceptionType::INVALID, "Cannot add values of this type");
}

auto Type::Subtract(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
    -> Value {
  throw NotImplementedException("Subtract not implemented");
}

auto Type::Multiply(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
    -> Value {
  throw NotImplementedException("Multiply not implemented");
}

auto Type::Divide(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
    -> Value {
  throw NotImplementedException("Divide not implemented");
}

auto Type::Modulo(const Value &left __attribute__((unused)), const Value &right __attribute__((unused))) const
    -> Value {
  throw NotImplementedException("Modulo not implemented");
}

auto Type::Min(const Value &left, const Value &right) const -> Value {
  if (left.type_id_ != right.type_id_) {
    throw Exception(ExceptionType::MISMATCH_TYPE, "type of left and right does not match");
  }

  switch (left.type_id_) {
    case TINYINT:
      return Value(left.type_id_, static_cast<int8_t>(std::min(left.value_.tinyint_, right.value_.tinyint_)));
    case SMALLINT:
      return Value(left.type_id_, static_cast<int16_t>(std::min(left.value_.smallint_, right.value_.smallint_)));
    case INTEGER:
      return Value(left.type_id_, static_cast<int32_t>(std::min(left.value_.integer_, right.value_.integer_)));
    case BIGINT:
      return Value(left.type_id_, static_cast<int64_t>(std::min(left.value_.bigint_, right.value_.bigint_)));
    case DECIMAL:
      return Value(left.type_id_, static_cast<double>(std::min(left.value_.decimal_, right.value_.decimal_)));
    case TIMESTAMP:
      return Value(left.type_id_, static_cast<uint64_t>(std::min(left.value_.timestamp_, right.value_.timestamp_)));
    default:
      break;
  }
  throw Exception(ExceptionType::INVALID, "Cannot add value of type you have provided");
}

auto Type::Max(const Value &left, const Value &right) const -> Value {
  if (left.type_id_ != right.type_id_) {
    throw Exception(ExceptionType::MISMATCH_TYPE, "type of left and right does not match");
  }

  switch (left.type_id_) {
    case TINYINT:
      return Value(left.type_id_, static_cast<int8_t>(std::max(left.value_.tinyint_, right.value_.tinyint_)));
    case SMALLINT:
      return Value(left.type_id_, static_cast<int16_t>(std::max(left.value_.smallint_, right.value_.smallint_)));
    case INTEGER:
      return Value(left.type_id_, static_cast<int32_t>(std::max(left.value_.integer_, right.value_.integer_)));
    case BIGINT:
      return Value(left.type_id_, static_cast<int64_t>(std::max(left.value_.bigint_, right.value_.bigint_)));
    case DECIMAL:
      return Value(left.type_id_, static_cast<double>(std::max(left.value_.decimal_, right.value_.decimal_)));
    case TIMESTAMP:
      return Value(left.type_id_, static_cast<uint64_t>(std::max(left.value_.timestamp_, right.value_.timestamp_)));
    default:
      break;
  }
  throw Exception(ExceptionType::INVALID, "Cannot add value of type you have provided");
}

auto Type::Sqrt(const Value &val __attribute__((unused))) const -> Value {
  throw NotImplementedException("Sqrt not implemented");
}

auto Type::OperateNull(const Value &val __attribute__((unused)), const Value &right __attribute__((unused))) const
    -> Value {
  throw NotImplementedException("OperateNull not implemented");
}

auto Type::IsZero(const Value &val __attribute__((unused))) const -> bool {
  throw NotImplementedException("isZero not implemented");
}
/**
 * Is the data inlined into this classes storage space, or must it be accessed
 * through an indirection/pointer?
 */
auto Type::IsInlined(const Value &val __attribute__((unused))) const -> bool {
  throw NotImplementedException("IsLined not implemented");
}

/** Return a stringified version of this value*/
auto Type::ToString(const Value &val __attribute__((unused))) const -> std::string {
  throw NotImplementedException("ToString not implemented");
}

/**
 * Serialize this value into the given storage space. The inlined parameter
 * indicates whether we are allowed to inline this value into the storage
 * space, or whether we must store only a reference to this value. If inlined
 * is false, we may use the provided data pool to allocate space for this
 * value, storing a reference into the allocated pool space in the storage.
 */
void Type::SerializeTo(const Value &val __attribute__((unused)), char *storage __attribute__((unused))) const {
  throw NotImplementedException("SerializeTo not implemented");
}

/**
 * Deserialize a value of the given type from the given storage space.
 */
auto Type::DeserializeFrom(const char *storage __attribute__((unused))) const -> Value {
  throw NotImplementedException("DeserializeFrom not implemented");
}

/**
 * Create a copy of this value
 */
auto Type::Copy(const Value &val __attribute__((unused))) const -> Value {
  throw NotImplementedException("Copy not implemented");
}

auto Type::CastAs(const Value &val __attribute__((unused)), const TypeId type_id __attribute__((unused))) const
    -> Value {
  throw NotImplementedException("CastAs not implemented");
}

/**
 * Access the raw variable length data
 */
auto Type::GetData(const Value &val __attribute__((unused))) const -> const char * {
  throw NotImplementedException("GetData from value not implemented");
}

/**
 * Get the length of the variable length data
 */
auto Type::GetStorageSize(const Value &val __attribute__((unused))) const -> uint32_t {
  throw NotImplementedException("GetStorageSize not implemented");
}

}  // namespace bustub
