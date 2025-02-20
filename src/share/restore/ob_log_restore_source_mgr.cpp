/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE
#include "lib/restore/ob_storage.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_log_restore_struct.h"
#include "ob_log_restore_source_mgr.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/net/ob_addr.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"

using namespace oceanbase::share;
int ObLogRestoreSourceMgr::init(const uint64_t tenant_id, ObISQLClient *proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLogRestoreSourceMgr already init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(! is_user_tenant(tenant_id)) || OB_ISNULL(proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(proxy));
  } else if (OB_FAIL(table_operator_.init(tenant_id, proxy))) {
    LOG_WARN("table_operator_ init failed", K(ret), K(tenant_id), K(proxy));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObLogRestoreSourceMgr::update_recovery_until_scn(const SCN &recovery_until_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogRestoreSourceMgr not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!recovery_until_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid recovery_until_scn", KR(ret), K(recovery_until_scn));
  } else {
    ObLogRestoreSourceItem item(tenant_id_,
                                OB_DEFAULT_LOG_RESTORE_SOURCE_ID,
                                recovery_until_scn);
    if (OB_FAIL(table_operator_.update_source_until_scn(item))) {
      LOG_WARN("table_operator_ update_source_until_scn failed", K(ret), K(recovery_until_scn));
    } else {
      LOG_INFO("update log restore source recovery until ts succ", K(recovery_until_scn));
    }
  }
  return ret;
}

int ObLogRestoreSourceMgr::delete_source()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogRestoreSourceMgr not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(table_operator_.delete_source())) {
    LOG_WARN("table_operator_ delete_source failed", K(ret));
  } else {
    LOG_INFO("delete log restore source succ");
  }
  return ret;
}

int ObLogRestoreSourceMgr::add_service_source(const SCN &recovery_until_scn,
    const ObString &service_source)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogRestoreSourceMgr not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(service_source.empty() || !recovery_until_scn.is_valid()) ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(service_source), K(recovery_until_scn));
  } else {
    ObLogRestoreSourceItem item(tenant_id_,
                                OB_DEFAULT_LOG_RESTORE_SOURCE_ID,
                                ObLogRestoreSourceType::SERVICE,
                                service_source,
                                recovery_until_scn);
    if (OB_FAIL(table_operator_.insert_source(item))) {
      LOG_WARN("table_operator_ insert_source failed", K(ret), K(item));
    } else {
      LOG_INFO("add service source succ", K(recovery_until_scn), K(service_source));
    }
  }
  return ret;
}

int ObLogRestoreSourceMgr::add_location_source(const SCN &recovery_until_scn,
    const ObString &archive_dest)
{
  int ret = OB_SUCCESS;
  ObBackupDest dest;
  const int64_t MAX_RESTORE_CLUSTER_STR = 10 + 16;                        // CLUSTER_ID=4294967295
  const int64_t MAX_RESTORE_TENANT_STR = 20 + 16;                         // TENANT_ID=18446744073709551615
  const int64_t MAX_RESTORE_LOCAION_STR = OB_MAX_BACKUP_DEST_LENGTH + 16; // LOCATION=file:///data/xxxx
  char dest_buf[MAX_RESTORE_LOCAION_STR + MAX_RESTORE_CLUSTER_STR + MAX_RESTORE_TENANT_STR] = { 0 };
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogRestoreSourceMgr not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(archive_dest.empty() || !recovery_until_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(archive_dest), K(recovery_until_scn));
  } else if (OB_FAIL(dest.set(archive_dest.ptr()))) {
    // use backup dest to manage oss key
    LOG_WARN("set backup dest failed", K(ret), K(archive_dest));
  } else if (OB_FAIL(dest.get_backup_dest_str_with_primary_attr(dest_buf, sizeof(dest_buf)))) {
    // store primary cluster id and tenant id in log restore source
    LOG_WARN("get backup dest str with primary attr failed", K(ret), K(dest));
  } else {
    ObLogRestoreSourceItem item(tenant_id_,
                                OB_DEFAULT_LOG_RESTORE_SOURCE_ID,
                                ObLogRestoreSourceType::LOCATION,
                                ObString(dest_buf),
                                recovery_until_scn);
    if (OB_FAIL(table_operator_.insert_source(item))) {
      LOG_WARN("table_operator_ insert_source failed", K(ret), K(item));
    } else {
      LOG_INFO("add location source succ", K(recovery_until_scn), K(archive_dest), K(item));
    }
  }
  return ret;
}

int ObLogRestoreSourceMgr::add_rawpath_source(const SCN &recovery_until_scn, const DirArray &array)
{
  return OB_NOT_SUPPORTED;
}

int ObLogRestoreSourceMgr::get_source(ObLogRestoreSourceItem &item)
{
  int ret = OB_SUCCESS;
  // only support src_id 1
  item.tenant_id_ = tenant_id_;
  item.id_ = OB_DEFAULT_LOG_RESTORE_SOURCE_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogRestoreSourceMgr not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(table_operator_.get_source(item))) {
    LOG_WARN("table_operator_ get_source failed", K(ret));
  } else {
    LOG_TRACE("get_source succ", K(item));
  }
  return ret;
}

int ObLogRestoreSourceMgr::get_source_for_update(ObLogRestoreSourceItem &item, common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  // only support src_id 1
  item.tenant_id_ = tenant_id_;
  item.id_ = OB_DEFAULT_LOG_RESTORE_SOURCE_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogRestoreSourceMgr not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(table_operator_.get_source_for_update(item, trans))) {
    LOG_WARN("table_operator_ get_source failed", K(ret));
  } else {
    LOG_TRACE("get_source succ", K(item));
  }
  return ret;
}

int ObLogRestoreSourceMgr::get_backup_dest(const ObLogRestoreSourceItem &item, ObBackupDest &dest)
{
  int ret = OB_SUCCESS;
  ObRestoreSourceLocationPrimaryAttr location_attr;
  if (OB_UNLIKELY(! item.is_valid() || ! is_location_log_source_type(item.type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else if (OB_FAIL(location_attr.parse_location_attr_from_str(item.value_))) {
    LOG_WARN("parse location attr from string failed", K(item));
  } else if OB_FAIL(dest.set(location_attr.location_.ptr())) {
    LOG_WARN("backup dest set failed", K(ret), K(item));
  }
  return ret;
}
