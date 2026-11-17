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

#include "config_bridge.h"
#include "observer/ai_service/ob_ai_service_executor.h"
#include "share/ob_dml_sql_splicer.h"
#include "observer/ai_service/ob_ai_service_proxy.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_server_struct.h"
#include "share/ob_max_id_fetcher.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "lib/string/ob_sql_string.h"

#define USING_LOG_PREFIX SHARE

using namespace oceanbase::observer;
using namespace oceanbase::transaction::tablelock;

namespace oceanbase
{
namespace share
{

namespace {
int append_ai_sql_identifier(ObSqlString &sql, const ObString &name)
{
  int ret = sql.append("`");
  for (int64_t i = 0; OB_SUCC(ret) && i < name.length(); ++i) {
    if ('`' == name.ptr()[i] && OB_FAIL(sql.append("`"))) {
    } else if (OB_FAIL(sql.append_fmt("%c", name.ptr()[i]))) {
    }
  }
  if (OB_SUCC(ret)) { ret = sql.append("`"); }
  return ret;
}

int append_ai_sql_hex(ObSqlString &sql, const ObString &value)
{
  int ret = sql.append("X'");
  for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
    if (OB_FAIL(sql.append_fmt("%02X", static_cast<unsigned char>(value.ptr()[i])))) {
    }
  }
  if (OB_SUCC(ret)) { ret = sql.append("'"); }
  return ret;
}

int local_get_runtime_name_case_mode(ObNameCaseMode &name_case_mode)
{
  int ret = OB_SUCCESS;
  share::schema::ObMultiVersionSchemaService *schema_service = nullptr;
  if (OB_ISNULL(schema_service = GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "schema service must not be null", K(ret));
  } else if (!schema_service->is_runtime_schema_refreshed()) {
    ret = OB_SCHEMA_EAGAIN;
    OB_LOG(WARN, "wait schema refreshed", K(ret));
  } else if (OB_FAIL(schema_service->get_runtime_name_case_mode(name_case_mode))) {
  }
  return ret;
}

int local_check_ai_model_exists(const ObString &ai_model_name)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  const schema::ObAiModelSchema *ai_model_schema = nullptr;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(schema_service->get_runtime_schema_guard(guard))) {
  } else if (OB_FAIL(guard.get_ai_model_schema(ai_model_name, ai_model_schema))) {
  } else if (OB_ISNULL(ai_model_schema)) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(
        OB_AI_FUNC_PARAM_VALUE_INVALID,
        strlen("ai_model_name"),
        "ai_model_name");
  }
  return ret;
}
}
const int64_t ObAiServiceExecutor::SPECIAL_ENDPOINT_ID_FOR_VERSION = -1;
const int64_t ObAiServiceExecutor::INIT_ENDPOINT_VERSION = 0;
const char *ObAiServiceExecutor::SPECIAL_ENDPOINT_SCOPE_FOR_VERSION = "";

int ObAiServiceExecutor::run_ai_column_task_once()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    return OB_NOT_INIT;
  }
  ObArenaAllocator allocator("AiColWorker");
  uint64_t table_id = 0;
  uint64_t column_id = 0;
  int64_t retry_count = 0;
  int64_t max_retries = 0;
  ObString request_id;
  ObString predicate;
  ObString expression;
  ObString column_name;
  ObString table_name;
  ObString database_name;
  bool found = false;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    const char *sql =
        "SELECT task.table_id, task.column_id, task.request_id, "
        "task.rowkey_predicate, task.retry_count, ai.max_retries, "
        "ai.normalized_expr, ai.column_name, t.table_name, d.database_name "
        "FROM oceanbase.__all_ai_column_task task "
        "JOIN oceanbase.__all_ai_column ai ON ai.table_id=task.table_id "
        "AND ai.column_id=task.column_id "
        "JOIN oceanbase.__all_table t ON t.table_id=task.table_id "
        "JOIN oceanbase.__all_database d ON d.database_id=t.database_id "
        "WHERE task.state IN (0,1,3) AND task.eligible_ts<=NOW(6) "
        "ORDER BY task.eligible_ts LIMIT 1";
    int64_t id_value = 0;
    ObString value;
    if (OB_FAIL(GCTX.sql_proxy_->read(res, sql))) {
    } else if (OB_ISNULL(res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(res.get_result()->next())) {
      if (OB_ITER_END == ret) { ret = OB_SUCCESS; }
    } else {
      found = true;
      if (OB_FAIL(res.get_result()->get_int("table_id", id_value))) {
      } else if (FALSE_IT(table_id = static_cast<uint64_t>(id_value))) {
      } else if (OB_FAIL(res.get_result()->get_int("column_id", id_value))) {
      } else if (FALSE_IT(column_id = static_cast<uint64_t>(id_value))) {
      } else if (OB_FAIL(res.get_result()->get_int("retry_count", retry_count))) {
      } else if (OB_FAIL(res.get_result()->get_int("max_retries", max_retries))) {
      } else if (OB_FAIL(res.get_result()->get_varchar("request_id", value))) {
      } else if (OB_FAIL(ob_write_string(allocator, value, request_id))) {
      } else if (OB_FAIL(res.get_result()->get_varchar("rowkey_predicate", value))) {
      } else if (OB_FAIL(ob_write_string(allocator, value, predicate))) {
      } else if (OB_FAIL(res.get_result()->get_varchar("normalized_expr", value))) {
      } else if (OB_FAIL(ob_write_string(allocator, value, expression))) {
      } else if (OB_FAIL(res.get_result()->get_varchar("column_name", value))) {
      } else if (OB_FAIL(ob_write_string(allocator, value, column_name))) {
      } else if (OB_FAIL(res.get_result()->get_varchar("table_name", value))) {
      } else if (OB_FAIL(ob_write_string(allocator, value, table_name))) {
      } else if (OB_FAIL(res.get_result()->get_varchar("database_name", value))) {
      } else if (OB_FAIL(ob_write_string(allocator, value, database_name))) {
      }
    }
    if (OB_NOT_NULL(res.get_result())) {
      const int close_ret = res.get_result()->close();
      if (OB_SUCC(ret) && OB_SUCCESS != close_ret) { ret = close_ret; }
    }
  }
  if (OB_FAIL(ret) || !found) {
    return ret;
  }

  ObSqlString task_key;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(task_key.assign_fmt("table_id=%lu AND column_id=%lu AND request_id=",
                                   static_cast<unsigned long>(table_id),
                                   static_cast<unsigned long>(column_id)))) {
  } else if (OB_FAIL(append_ai_sql_hex(task_key, request_id))) {
  } else if (OB_FAIL(sql.assign_fmt(
                 "UPDATE oceanbase.__all_ai_column_task SET state=1, "
                 "eligible_ts=DATE_ADD(NOW(6),INTERVAL 10 SECOND) "
                 "WHERE %.*s AND state IN (0,1,3) AND eligible_ts<=NOW(6)",
                 task_key.length(), task_key.ptr()))) {
  } else if (OB_FAIL(GCTX.sql_proxy_->write(sql.ptr(), affected_rows))) {
  }
  if (OB_FAIL(ret) || 1 != affected_rows) {
    return ret;
  }

  ObSqlString value_query;
  ObString generated_value;
  bool row_exists = false;
  if (OB_FAIL(value_query.append("SELECT CAST(("))) {
  } else if (OB_FAIL(value_query.append_fmt("%.*s", expression.length(), expression.ptr()))) {
  } else if (OB_FAIL(value_query.append(") AS CHAR) AS generated_value FROM "))) {
  } else if (OB_FAIL(append_ai_sql_identifier(value_query, database_name))) {
  } else if (OB_FAIL(value_query.append("."))) {
  } else if (OB_FAIL(append_ai_sql_identifier(value_query, table_name))) {
  } else if (OB_FAIL(value_query.append_fmt(" WHERE %.*s LIMIT 1",
                                             predicate.length(), predicate.ptr()))) {
  }
  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, value_res) {
      ObString value;
      if (OB_FAIL(GCTX.sql_proxy_->read(value_res, value_query.ptr()))) {
      } else if (OB_ISNULL(value_res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(value_res.get_result()->next())) {
        if (OB_ITER_END == ret) { ret = OB_SUCCESS; }
      } else if (OB_FAIL(value_res.get_result()->get_varchar("generated_value", value))) {
      } else if (OB_FAIL(ob_write_string(allocator, value, generated_value))) {
      } else {
        row_exists = true;
      }
      if (OB_NOT_NULL(value_res.get_result())) {
        const int close_ret = value_res.get_result()->close();
        if (OB_SUCC(ret) && OB_SUCCESS != close_ret) { ret = close_ret; }
      }
    }
  }

  const bool model_failed = OB_FAIL(ret);
  if (model_failed && retry_count < max_retries) {
    sql.reset();
    int retry_ret = sql.assign_fmt(
        "UPDATE oceanbase.__all_ai_column_task SET state=3, "
        "retry_count=retry_count+1, "
        "eligible_ts=DATE_ADD(NOW(6),INTERVAL 1 SECOND) "
        "WHERE %.*s AND state=1", task_key.length(), task_key.ptr());
    if (OB_SUCCESS == retry_ret) {
      int64_t rows = 0;
      retry_ret = GCTX.sql_proxy_->write(sql.ptr(), rows);
    }
    return retry_ret;
  }

  ObMySQLTransaction trans;
  int publish_ret = trans.start(GCTX.sql_proxy_);
  bool task_exists = false;
  if (OB_SUCCESS == publish_ret) {
    sql.reset();
    publish_ret = sql.assign_fmt(
        "SELECT 1 FROM oceanbase.__all_ai_column_task WHERE %.*s AND state=1 FOR UPDATE",
        task_key.length(), task_key.ptr());
  }
  if (OB_SUCCESS == publish_ret) {
    SMART_VAR(ObMySQLProxy::MySQLResult, check_res) {
      if (OB_SUCCESS != (publish_ret = trans.read(check_res, sql.ptr()))) {
      } else if (OB_ISNULL(check_res.get_result())) {
        publish_ret = OB_ERR_UNEXPECTED;
      } else if (OB_SUCCESS == (publish_ret = check_res.get_result()->next())) {
        task_exists = true;
      } else if (OB_ITER_END == publish_ret) {
        publish_ret = OB_SUCCESS;
      }
      if (OB_NOT_NULL(check_res.get_result())) {
        const int close_ret = check_res.get_result()->close();
        if (OB_SUCCESS == publish_ret && OB_SUCCESS != close_ret) { publish_ret = close_ret; }
      }
    }
  }
  if (OB_SUCCESS == publish_ret && task_exists && row_exists) {
    sql.reset();
    if (OB_SUCCESS != (publish_ret = sql.append("UPDATE "))) {
    } else if (OB_SUCCESS != (publish_ret = append_ai_sql_identifier(sql, database_name))) {
    } else if (OB_SUCCESS != (publish_ret = sql.append("."))) {
    } else if (OB_SUCCESS != (publish_ret = append_ai_sql_identifier(sql, table_name))) {
    } else if (OB_SUCCESS != (publish_ret = sql.append(" SET "))) {
    } else if (OB_SUCCESS != (publish_ret = append_ai_sql_identifier(sql, column_name))) {
    } else if (OB_SUCCESS != (publish_ret = sql.append("="))) {
    } else if (model_failed) {
      publish_ret = sql.append("NULL");
    } else if (OB_SUCCESS != (publish_ret = append_ai_sql_hex(sql, generated_value))) {
    }
    if (OB_SUCCESS == publish_ret) {
      if (OB_SUCCESS != (publish_ret = sql.append_fmt(
              ", `__ai_state_%lu`=%d WHERE %.*s",
              static_cast<unsigned long>(column_id), model_failed ? 2 : 1,
              predicate.length(), predicate.ptr()))) {
      } else if (OB_SUCCESS != (publish_ret = trans.write(sql.ptr(), affected_rows))) {
      }
    }
  }
  if (OB_SUCCESS == publish_ret && task_exists) {
    sql.reset();
    if (OB_SUCCESS != (publish_ret = sql.assign_fmt(
            "DELETE FROM oceanbase.__all_ai_column_task WHERE %.*s AND state=1",
            task_key.length(), task_key.ptr()))) {
    } else if (OB_SUCCESS != (publish_ret = trans.write(sql.ptr(), affected_rows))) {
    }
  }
  if (trans.is_started()) {
    const int end_ret = trans.end(OB_SUCCESS == publish_ret);
    if (OB_SUCCESS == publish_ret) { publish_ret = end_ret; }
  }
  return publish_ret;
}

int ObAiServiceExecutor::create_ai_model_endpoint(common::ObArenaAllocator &allocator, const ObString &endpoint_name, const ObIJsonBase &create_jbase)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObAiModelEndpointInfo endpoint;
  uint64_t new_endpoint_id = OB_INVALID_ID;
  
  int64_t new_endpoint_version = OB_INVALID_VERSION;
  bool is_exists = false;
  ObAiModelEndpointInfo tmp_endpoint;
  if (OB_FAIL(endpoint.parse_from_json_base(allocator, endpoint_name, create_jbase))) {
  } else if (OB_FAIL(local_check_ai_model_exists(endpoint.get_ai_model_name()))) {
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
  } else if (OB_FAIL(ObAiServiceProxy::check_ai_endpoint_exists(allocator, trans, endpoint_name, is_exists))) {
  } else if (is_exists) {
    ret = OB_AI_FUNC_ENDPOINT_EXISTS;
    LOG_USER_ERROR(OB_AI_FUNC_ENDPOINT_EXISTS, endpoint_name.length(), endpoint_name.ptr());
  } else {
    // check if the ai model endpoint has the same ai model name is already exists
    if (OB_FAIL(read_ai_endpoint_by_ai_model_name(allocator, endpoint.get_ai_model_name(), tmp_endpoint))) {
      if (ret == OB_AI_FUNC_ENDPOINT_NOT_FOUND) {
        ret = OB_SUCCESS;
      } else {
      }
    } else {
      ret = OB_AI_FUNC_ENDPOINT_EXISTS;
      FORWARD_USER_ERROR_MSG(OB_AI_FUNC_ENDPOINT_EXISTS, "The ai model endpoint '%.*s' has the same ai model name '%.*s'", tmp_endpoint.get_name().length(), tmp_endpoint.get_name().ptr(), tmp_endpoint.get_ai_model_name().length(), tmp_endpoint.get_ai_model_name().ptr());
    }
  }
  
  
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fetch_new_ai_model_endpoint_id( new_endpoint_id))) {
  } else if (FALSE_IT(endpoint.set_endpoint_id(new_endpoint_id))) {
  } else if (OB_FAIL(lock_and_fetch_endpoint_version(trans, new_endpoint_version))) {
  } else if (OB_FAIL(ObAiServiceProxy::insert_ai_endpoint( trans, new_endpoint_version, endpoint))) {
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObAiServiceExecutor::alter_ai_model_endpoint(ObArenaAllocator &allocator, const ObString &name, const ObIJsonBase &alter_jbase)
{
  int ret = OB_SUCCESS;
  ObAiModelEndpointInfo old_endpoint;
  ObAiModelEndpointInfo new_endpoint;
  ObMySQLTransaction trans;
  ObAiModelEndpointInfo tmp_endpoint;
  ObNameCaseMode name_case_mode;
  
   // AI model names may be case-sensitive, so use the runtime name-case mode.
  int64_t new_endpoint_version = OB_INVALID_VERSION;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
  } else if (OB_FAIL(ObAiServiceProxy::select_ai_endpoint(allocator, *GCTX.sql_proxy_, name, old_endpoint, true))) {
  } else if (OB_FAIL(construct_new_endpoint(allocator, old_endpoint, alter_jbase, new_endpoint))) {
  } else if (OB_FAIL(new_endpoint.check_valid())) {
  } else if (OB_FAIL(local_get_runtime_name_case_mode(name_case_mode))) {
  } else if (ObCharset::case_mode_equal(name_case_mode, new_endpoint.get_ai_model_name(), old_endpoint.get_ai_model_name())) {
    // need check name case mode equal, if not change ai model name, just update the endpoint 
    LOG_INFO("ai model name is the same, just update the endpoint", KR(ret), K(name), K(name_case_mode), K(new_endpoint), K(old_endpoint));
  } else if (OB_FAIL(local_check_ai_model_exists(new_endpoint.get_ai_model_name()))) {
  } else {
    // if change ai model name, check if the ai model endpoint has the same ai model name is already exists
    // if not exists, continue
    if (OB_FAIL(read_ai_endpoint_by_ai_model_name(allocator, new_endpoint.get_ai_model_name(), tmp_endpoint))) {
      if (ret == OB_AI_FUNC_ENDPOINT_NOT_FOUND) {
        ret = OB_SUCCESS;
      } else {
      }
    } else {
      ret = OB_AI_FUNC_ENDPOINT_EXISTS;
      FORWARD_USER_ERROR_MSG(OB_AI_FUNC_ENDPOINT_EXISTS, "The ai model endpoint '%.*s' has the same ai model name '%.*s'", tmp_endpoint.get_name().length(), tmp_endpoint.get_name().ptr(), tmp_endpoint.get_ai_model_name().length(), tmp_endpoint.get_ai_model_name().ptr());
    }
  }
  
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(lock_and_fetch_endpoint_version(trans, new_endpoint_version))) {
  } else if (OB_FAIL(ObAiServiceProxy::update_ai_endpoint( trans, new_endpoint_version, new_endpoint))) {
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  return ret;
}

int ObAiServiceExecutor::construct_new_endpoint(common::ObArenaAllocator &allocator,
                                                const ObAiModelEndpointInfo &old_endpoint,
                                                const ObIJsonBase &alter_jbase,
                                                ObAiModelEndpointInfo &new_endpoint)
{
  int ret = OB_SUCCESS;
  new_endpoint = old_endpoint;
  if (OB_FAIL(new_endpoint.merge_delta_endpoint(allocator, alter_jbase))) {
  } 
  return ret;
}

int ObAiServiceExecutor::drop_ai_model_endpoint(const ObString &endpoint_name)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  
  int64_t new_endpoint_version = OB_INVALID_VERSION;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
  } else if (OB_FAIL(lock_and_fetch_endpoint_version(trans, new_endpoint_version))) {
  } else if (OB_FAIL(ObAiServiceProxy::drop_ai_model_endpoint(trans, endpoint_name))) {
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObAiServiceExecutor::read_ai_endpoint(ObArenaAllocator &allocator, const ObString &endpoint_name, ObAiModelEndpointInfo &endpoint_info)
{
  int ret = OB_SUCCESS;
  
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObAiServiceProxy::select_ai_endpoint(allocator, *GCTX.sql_proxy_, endpoint_name, endpoint_info))) {
  }
  return ret;
}

int ObAiServiceExecutor::read_ai_endpoint_by_ai_model_name(ObArenaAllocator &allocator, const ObString &ai_model_name, ObAiModelEndpointInfo &endpoint_info)
{
  int ret = OB_SUCCESS;
  
  
  ObNameCaseMode name_case_mode;
  if (OB_FAIL(local_get_runtime_name_case_mode(name_case_mode))) {
  } else if (OB_NAME_CASE_INVALID >= name_case_mode || OB_NAME_CASE_MAX <= name_case_mode) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObAiServiceProxy::select_ai_endpoint_by_ai_model_name(allocator, *GCTX.sql_proxy_, ai_model_name, name_case_mode, endpoint_info))) {
  }
  return ret;
}

int ObAiServiceExecutor::fetch_new_ai_model_endpoint_id(uint64_t &new_ai_model_endpoint_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObMaxIdFetcher fetcher(*GCTX.sql_proxy_);
    if (OB_FAIL(fetcher.fetch_new_max_id( OB_MAX_USED_AI_MODEL_ENDPOINT_ID_TYPE, new_ai_model_endpoint_id, 0))) {
    }
  }
  return ret;
}

int ObAiServiceExecutor::lock_and_fetch_endpoint_version(ObMySQLTransaction &trans, int64_t &endpoint_version)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = config::internal_sql_execute_timeout();
  observer::ObInnerSQLConnection *conn = NULL;
  ObSqlString sql;
  
  int64_t old_endpoint_version = OB_INVALID_VERSION;
  int64_t new_endpoint_version = OB_INVALID_VERSION;
  bool need_insert = false;

  if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObInnerConnectionLockUtil::lock_table(OB_ALL_AI_MODEL_ENDPOINT_TID,
                                                           EXCLUSIVE,
                                                           timeout,
                                                           conn))) {
  } else if (OB_FAIL(sql.assign_fmt("SELECT VERSION FROM %s WHERE endpoint_id = %ld AND scope = '%s'",
      OB_ALL_AI_MODEL_ENDPOINT_TNAME, SPECIAL_ENDPOINT_ID_FOR_VERSION, SPECIAL_ENDPOINT_SCOPE_FOR_VERSION))) {
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      int tmp_ret = OB_SUCCESS;
      const int64_t idx = 0;
      if (OB_FAIL(trans.read(res, sql.ptr()))) {
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          need_insert = true;
          res.reset();
        } else {
        }
      } else if (OB_FAIL(result->get_int(idx, old_endpoint_version))) {
      } else if (OB_ITER_END != (tmp_ret = result->next())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
      } else {
        res.reset();
        new_endpoint_version = old_endpoint_version + 1;
      }
    }

    if (OB_SUCC(ret) && need_insert) {
      if (OB_FAIL(insert_special_endpoint_for_version(trans))) {
      } else {
        new_endpoint_version = INIT_ENDPOINT_VERSION + 1;
      }
    }

    if (OB_SUCC(ret)) {
      ObDMLSqlSplicer sql;
      ObSqlString buffer;
      int64_t affected_rows = 0;
      if (OB_FAIL(sql.add_pk_column("endpoint_id", SPECIAL_ENDPOINT_ID_FOR_VERSION))) {
      } else if (OB_FAIL(sql.add_pk_column("scope", SPECIAL_ENDPOINT_SCOPE_FOR_VERSION))) {
      } else if (OB_FAIL(sql.add_column("version", new_endpoint_version))) {
      } else if (OB_FAIL(sql.splice_update_sql(OB_ALL_AI_MODEL_ENDPOINT_TNAME, buffer))) {
      } else if (OB_FAIL(trans.write(buffer.ptr(), affected_rows))) {
      } else if (1 != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }

  if (OB_SUCC(ret)) {
    endpoint_version = new_endpoint_version;
  }
  return ret;
}

int ObAiServiceExecutor::insert_special_endpoint_for_version(ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  
  uint64_t new_endpoint_version = OB_INVALID_VERSION;
  int64_t affected_rows = 0;

  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  if (OB_FAIL(sql.add_pk_column("endpoint_id", SPECIAL_ENDPOINT_ID_FOR_VERSION))) {
  } else if (OB_FAIL(sql.add_pk_column("scope", SPECIAL_ENDPOINT_SCOPE_FOR_VERSION))) {
  } else if (OB_FAIL(sql.add_column("version", INIT_ENDPOINT_VERSION))) {
  } else if (OB_FAIL(sql.add_column("endpoint_name", ""))) {
  } else if (OB_FAIL(sql.add_column("ai_model_name", ""))) {
  } else if (OB_FAIL(sql.add_column("url", ""))) {
  } else if (OB_FAIL(sql.add_column("access_key", ""))) {
  } else if (OB_FAIL(sql.add_column("request_model_name", ""))) {
  } else if (OB_FAIL(sql.add_column("provider", ""))) {
  } else if (OB_FAIL(sql.add_column("parameters", ""))) {
  } else if (OB_FAIL(sql.add_column("request_transform_fn", ""))) {
  } else if (OB_FAIL(sql.add_column("response_transform_fn", ""))) {
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_AI_MODEL_ENDPOINT_TNAME, buffer))) {
  } else if (OB_FAIL(trans.write(buffer.ptr(), affected_rows))) {
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}


} // namespace share
} // namespace oceanbase
