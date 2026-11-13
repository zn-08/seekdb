/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_obj_cast_runtime.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/session/ob_basic_session_info.h"
#include <limits.h>
#include <string>
#include <vector>


namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObFunctionTableSpec, ObOpSpec), value_expr_, column_exprs_, has_correlated_expr_);

int ObFunctionTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  already_calc_ = false;
  if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (T_FUN_SYS_READ_CSV == MY_SPEC.value_expr_->type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_csv;
    ret = inner_open_csv();
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_udf;
  } else {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObFunctionTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else if (OB_NOT_NULL(MY_SPEC.value_expr_)
             && T_FUN_SYS_READ_CSV == MY_SPEC.value_expr_->type_) {
    if (!csv_reader_.rescan()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("CSV file changed before rescan", K(ret), "reason", csv_reader_.error().c_str());
    }
  } else {
    node_idx_ = 0;
    if (MY_SPEC.has_correlated_expr_) {
      row_count_ = 0;
      col_count_ = 0;
      value_table_ = NULL;
      already_calc_ = false;
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  value_table_ = NULL;
  csv_reader_.close();
  return ret;
}

int ObFunctionTableOp::inner_open_csv()
{
  int ret = OB_SUCCESS;
  ObDatum *path_datum = NULL;
  ObString secure_file_priv;
  char actual_path[PATH_MAX] = {0};
  if (MY_SPEC.value_expr_->arg_cnt_ != 1 || OB_ISNULL(MY_SPEC.value_expr_->args_[0])) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(MY_SPEC.value_expr_->args_[0]->eval(eval_ctx_, path_datum))) {
  } else if (OB_ISNULL(path_datum) || path_datum->is_null()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObString path_value = path_datum->get_string();
    const std::string path(path_value.ptr(), path_value.length());
    if (path.empty() || path.find('\0') != std::string::npos) {
      ret = OB_INVALID_ARGUMENT;
#ifdef _WIN32
    } else if (OB_ISNULL(::_fullpath(actual_path, path.c_str(), sizeof(actual_path)))) {
#else
    } else if (OB_ISNULL(::realpath(path.c_str(), actual_path))) {
#endif
      ret = OB_FILE_NOT_EXIST;
    } else if (OB_ISNULL(GET_MY_SESSION(ctx_))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(GET_MY_SESSION(ctx_)->get_secure_file_priv(secure_file_priv))) {
    } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv, ObString(actual_path)))) {
    } else if (!csv_reader_.open(actual_path)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to open CSV table", K(ret), "reason", csv_reader_.error().c_str());
    } else if (csv_reader_.columns().size() != MY_SPEC.column_exprs_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("CSV schema changed after query resolution", K(ret));
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_csv()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  std::vector<std::string> row;
  const FileSQLCsvReader::Status status = csv_reader_.next(row);
  if (status == FileSQLCsvReader::Status::END) {
    ret = OB_ITER_END;
  } else if (status == FileSQLCsvReader::Status::ERROR) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to read CSV row", K(ret), "reason", csv_reader_.error().c_str(),
             "row", csv_reader_.row_number());
  } else if (row.size() != MY_SPEC.column_exprs_.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(GET_MY_SESSION(ctx_));
    for (size_t i = 0; OB_SUCC(ret) && i < row.size(); ++i) {
      ObExpr *column_expr = MY_SPEC.column_exprs_.at(i);
      if (OB_ISNULL(column_expr)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObDatum &datum = column_expr->locate_datum_for_write(eval_ctx_);
        if (row[i].empty()) {
          datum.set_null();
        } else {
          ObObj source;
          ObObj converted;
          source.set_varchar(ObString(static_cast<int32_t>(row[i].size()), row[i].data()));
          source.set_collation_type(CS_TYPE_UTF8MB4_BIN);
          ObAccuracy accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][column_expr->obj_meta_.get_type()];
          ObCastCtx cast_ctx(&ctx_.get_allocator(), &dtc_params, CM_NONE,
                             column_expr->obj_meta_.get_collation_type(), &accuracy);
          ObSqlObjCastRuntime cast_runtime(&ctx_);
          cast_ctx.runtime_ = &cast_runtime;
          if (OB_FAIL(ObObjCaster::to_type(column_expr->obj_meta_.get_type(),
                                         column_expr->obj_meta_.get_collation_type(),
                                         cast_ctx, source, converted))) {
          } else if (OB_FAIL(datum.from_obj(converted, column_expr->obj_datum_map_))) {
          }
        }
        if (OB_SUCC(ret)) {
          column_expr->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

void ObFunctionTableOp::destroy()
{
  ObOperator::destroy();
}


int ObFunctionTableOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result in table function",
              K(node_idx_), K(row_count_), K(col_count_));
  }
  do {
    CK (node_idx_ >= 0);
    if (OB_SUCC(ret) && node_idx_ >= row_count_) {
      ret = OB_ITER_END;
    }
    CK (OB_NOT_NULL(value_table_));
    OX (data = value_table_->get_data());
    CK (OB_NOT_NULL(data));
    OX (result = (static_cast<ObObj*>(data))[node_idx_++]);
  } while (OB_SUCC(ret) && result.get_meta().get_type() == ObMaxType);
  return ret;
}

int ObFunctionTableOp::inner_get_next_row()
{
  return (this->*next_row_func_)();
}

int ObFunctionTableOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
      } else if (value->is_null()) {
        //do nothing
      } else if (OB_ISNULL(value_table_ 
                 = reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        row_count_ = value_table_->is_inited() ? value_table_->get_count() : 0;
        col_count_ = value_table_->get_column_count();
      }
      already_calc_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(node_idx_ >= row_count_)) {
      ret = OB_ITER_END;
    }
    CK (MY_SPEC.column_exprs_.count() >= col_count_);
    ObObj obj_stack[col_count_];
    if (OB_SUCC(ret)) {
      if (nullptr != value_table_ 
          && ObExtendType == value_table_->get_element_type().get_obj_type()) {
        pl::ObPLComposite *composite = NULL;
        pl::ObPLRecord *record = NULL;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;   
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite = reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
          if (OB_SUCC(ret)) {
            if (composite->is_record()) {
              OX (record = static_cast<pl::ObPLRecord*>(composite));
              CK (record->get_count() == col_count_);
              for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
                OZ (record->get_element(i, obj_stack[i]));
              }
            } else if (composite->is_collection()) {
              OX (obj_stack[0] = record_obj);
            } else {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected here", K(ret), K(record_obj), K(record_obj.meta_));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
          if (OB_FAIL(get_current_result(obj_stack[i]))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        if (obj_stack[i].is_null()) {
          MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_).set_null();
        } else {
          const ObObjDatumMapType &datum_map = MY_SPEC.column_exprs_.at(i)->obj_datum_map_;
          ObExpr * const &expr = MY_SPEC.column_exprs_.at(i);
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(obj_stack[i], datum_map))) {
          } else if (is_lob_storage(obj_stack[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(get_exec_ctx(), obj_stack[i],
                                                 expr->obj_meta_, datum_map,
                                                 get_exec_ctx().get_allocator(), datum))) {
          }
        }
        if (OB_SUCC(ret)) {
          MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
    if (OB_ITER_END != ret) {
    }
  } else {
    MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_datum(*value);
    MY_SPEC.column_exprs_.at(0)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
