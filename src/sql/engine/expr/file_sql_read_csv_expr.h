/*
 * Copyright (c) 2026 OceanBase.
 * Licensed under the Apache License, Version 2.0.
 */

#pragma once

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/ob_name_def.h"

namespace oceanbase
{
namespace sql
{

// This expression identifies the CSV table source in the logical and physical
// plans. Rows are produced by ObFunctionTableOp, never by scalar evaluation.
class FileSQLReadCsvExpr : public ObFuncExprOperator
{
public:
  explicit FileSQLReadCsvExpr(common::ObIAllocator &alloc)
      : FileSQLReadCsvExpr(alloc, T_FUN_SYS_READ_CSV, N_READ_CSV)
  {}

  int calc_result_type1(ObExprResType &type, ObExprResType &path,
                        common::ObExprTypeCtx &type_ctx) const override
  {
    UNUSED(type_ctx);
    path.set_calc_type(common::ObVarcharType);
    type.set_varchar();
    type.set_length(4096);
    type.set_collation_type(common::CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(common::CS_LEVEL_COERCIBLE);
    return common::OB_SUCCESS;
  }

  int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override
  {
    UNUSED(expr_cg_ctx);
    UNUSED(raw_expr);
    rt_expr.eval_func_ = FileSQLReadCsvExpr::eval_scalar;
    return common::OB_SUCCESS;
  }

  static int eval_scalar(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
  {
    UNUSED(expr);
    UNUSED(ctx);
    UNUSED(result);
    return common::OB_NOT_SUPPORTED;
  }

protected:
  FileSQLReadCsvExpr(common::ObIAllocator &alloc, ObExprOperatorType type, const char *name)
      : ObFuncExprOperator(alloc, type, name, 1,
                           NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
  {}
};

class FileSQLReadJsonExpr : public FileSQLReadCsvExpr
{
public:
  explicit FileSQLReadJsonExpr(common::ObIAllocator &alloc)
      : FileSQLReadCsvExpr(alloc, T_FUN_SYS_READ_JSON, N_READ_JSON)
  {}
};

class FileSQLReadNdjsonExpr : public FileSQLReadCsvExpr
{
public:
  explicit FileSQLReadNdjsonExpr(common::ObIAllocator &alloc)
      : FileSQLReadCsvExpr(alloc, T_FUN_SYS_READ_NDJSON, N_READ_NDJSON)
  {}
};

class FileSQLGlobExpr : public FileSQLReadCsvExpr
{
public:
  explicit FileSQLGlobExpr(common::ObIAllocator &alloc)
      : FileSQLReadCsvExpr(alloc, T_FUN_SYS_FILE_GLOB, N_FILE_GLOB)
  {}
};

} // namespace sql
} // namespace oceanbase
