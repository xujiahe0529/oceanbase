#define USING_LOG_PREFIX SQL_ENG
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "lib/oblog/ob_log.h"

#include "share/object/ob_obj_cast.h"
#include "share/config/ob_server_config.h"
#include "share/datum/ob_datum_util.h"
#include "objit/common/ob_item_type.h"

#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"

#include "storage/ob_storage_util.h"
#include "ob_expr_compress_model.h"
#include "sql/engine/python_udf_engine/ob_python_udf_op.h"

static const char* SAY_HELLO = "This is Compress Model.";

namespace oceanbase {
using namespace common;
namespace sql {

ObExprCompressModel::ObExprCompressModel(ObIAllocator& alloc) : ObExprOperator(alloc, T_FUN_SYS_COMPRESS_MODEL, N_COMPRESS_MODEL, MORE_THAN_ONE)
{}

ObExprCompressModel::~ObExprCompressModel()
{}

int ObExprCompressModel::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types_array,
                                       int64_t param_num,
                                       common::ObExprTypeCtx &type_ctx) const 
{
  UNUSED(type_ctx);
  type.set_int();
  //type.set_double();
  //type.set_varchar();
  //type.set_length(512);
  //type.set_default_collation_type();
  //type.set_collation_level(CS_LEVEL_SYSCONST);
  return  OB_SUCCESS;
}

int ObExprCompressModel::calc_resultN(common::ObObj &result,
                                  const common::ObObj *objs,
                                  int64_t param_num,
                                  common::ObExprCtx &expr_ctx) const
{
  UNUSED(expr_ctx);
  //result.set_varchar(common::ObString(SAY_HELLO));
  //result.set_collation(result_type_);
  return OB_SUCCESS;
}


int ObExprCompressModel::eval_compress_model(const ObExpr& expr, ObEvalCtx& ctx, ObDatum&  expr_datum)
{
  int ret = OB_SUCCESS;

  // 用户传入参数
  ObDatum *model_path = NULL;
  ObDatum *compress_path = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, model_path, compress_path))) {
    LOG_WARN("eval param value failed");
  }else{
    // 解析参数
    ObString model_path_val = model_path->get_string();
    ObString compress_path_val = compress_path->get_string();
    const int64_t c_str_len = 1000;
    char model_path_cstr[c_str_len];
    char compress_path_cstr[c_str_len];
    model_path_val.to_string(model_path_cstr, c_str_len);
    compress_path_val.to_string(compress_path_cstr, c_str_len);
    
    //获取当前工作线程tid并初始化引擎
    //pid_t tid = syscall(SYS_gettid);

    //调用python接口实现模型压缩功能
    //开启python解释器
    //pythonUdfEngine::init_python_udf_engine(tid);
    //initialize Python Intepreter
    Py_InitializeEx(!Py_IsInitialized());
    int judge = Py_IsInitialized();
    
    //Acquire GIL
    bool nStatus = PyGILState_Check();
    PyGILState_STATE gstate;
    if(!nStatus) {
      gstate = PyGILState_Ensure();
      nStatus = true;
    }
    
    //加载自定义模块
    PyRun_SimpleString("import sys");
    PyRun_SimpleString("sys.path.append('/home/python_scripts_path')");
    PyObject* prone = PyImport_ImportModule("prone");
    
    //获取调用的函数对象
    PyObject* prone_model_func = PyObject_GetAttrString(prone, "prone_model");

    //构造要传入的参数
    PyObject* pArgs = PyTuple_New(2);
    PyTuple_SetItem(pArgs, 0, PyUnicode_FromString(model_path_cstr));
    PyTuple_SetItem(pArgs, 1, PyUnicode_FromString(compress_path_cstr));

    //调用函数
    PyObject_CallObject(prone_model_func, pArgs);
  }

  
  

  // // 清理资源
  // Py_DECREF(pArgs);
  // Py_DECREF(prone_model_func);
  // Py_DECREF(prone);

  // // 关闭python解释器
  // Py_Finalize();
//   char pycall[] = "\
// \nimport numpy\
// \ndef prone_model(model_path, save_path): \
// \n\treturn 1";
//   PyObject *pModule, *pFunc, *pInitial, *dic, *v;
//   pModule = PyImport_AddModule("__main__");
//   if(!pModule)
//       return false;
//   dic = PyModule_GetDict(pModule);
//   if(!dic)
//       return false;
//   //test pycall
//   v = PyRun_StringFlags(pycall, Py_file_input, dic, dic, NULL);
//   if(!v)
//       return false;
//   //obtain function pointer called pFunc
//   pFunc = PyObject_GetAttrString(pModule, "prone_model");
//   if(!pFunc || !PyCallable_Check(pFunc))
//       return false;
//   // //构造要传入的参数
//   PyObject* pArgs = PyTuple_New(2);
//   PyTuple_SetItem(pArgs, 0, PyUnicode_FromString("/root/model_path/saved_model"));
//   PyTuple_SetItem(pArgs, 1, PyUnicode_FromString("/root/model_path/proned_model"));
//   PyObject_CallObject(pFunc, pArgs);

  return ret;
}


int ObExprCompressModel::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  //绑定eval
  rt_expr.eval_func_ = ObExprCompressModel::eval_compress_model;
  //绑定向量化eval
  // bool is_batch = true;
  // for(int i = 0; i < rt_expr.arg_cnt_; i++){
  //   if(!rt_expr.args_[i]->is_batch_result()) {
  //     is_batch = false;
  //     break;
  //   }
  // }
  // if(is_batch) {
  //   rt_expr.eval_batch_func_ = ObExprPythonUdf::eval_python_udf_batch_buffer;
  // }
  return  OB_SUCCESS;
}



}  // namespace sql
}  // namespace oceanbase