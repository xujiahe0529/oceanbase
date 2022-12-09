#define USING_LOG_PREFIX SQL_ENG

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "sql/engine/expr/ob_expr_python_udf.h"

#include "share/object/ob_obj_cast.h"
#include "share/config/ob_server_config.h"
#include "share/datum/ob_datum_util.h"

#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"


static const char* SAY_HELLO = "This is Python UDF.";

namespace oceanbase {
using namespace common;
namespace sql {

ObExprPythonUdf::ObExprPythonUdf(ObIAllocator& alloc) : ObExprOperator(alloc, T_FUN_SYS_PYTHON_UDF, N_PYTHON_UDF, PARAM_NUM_UNKNOWN)
{}

ObExprPythonUdf::~ObExprPythonUdf()
{}

int ObExprPythonUdf::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types_array,
                                       int64_t param_num,
                                       common::ObExprTypeCtx &type_ctx) const 
{
  UNUSED(type_ctx);

  type.set_varchar();
  type.set_length(512);
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_SYSCONST);

  for (int64_t i = 0; i < param_num; ++i) {
    types_array[i].set_calc_type(ObVarcharType);
    types_array[i].set_calc_collation_type(type.get_collation_type());
  }

  return  OB_SUCCESS;
}

int ObExprPythonUdf::calc_resultN(common::ObObj &result,
                                  const common::ObObj *objs_array,
                                  int64_t param_num,
                                  common::ObExprCtx &expr_ctx) const 
{
  UNUSED(expr_ctx);
  /*
  for (int64_t i = 0; i < param_num; ++i) {
    types_array[i].set_calc_type(ObVarcharType);
    types_array[i].set_calc_collation_type(type.get_collation_type());
  }
  */

  result.set_varchar(common::ObString(SAY_HELLO));
  result.set_collation(result_type_);
  return  OB_SUCCESS;
}


int ObExprPythonUdf::eval_python_udf(const  ObExpr&  expr, ObEvalCtx&  ctx, ObDatum&  expr_datum)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  /*
  ObString Code = common::OBString(
    "i = i * 2\n"+
    "return i"
    );

  int64_t num = 1;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    ObDatum *datum = NULL;
    if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {
      LOG_WARN("failed to eval datum", K(ret));
    } else if(i == 0){
      Code = datum->get_string();
    } else{
      num *= datum->get_int();
    }
  }
  */
  //PyObject *pName, *pModule, *pFunc;
  //PyObject *pArgs, *pValue;
  Py_InitializeEx(Py_IsInitialized());
  //PyRun_SimpleString("Test Python Interpreter.");
  PyObject* PyStr = PyBytes_FromString("Is this Python UDF?");
  char* result = PyBytes_AS_STRING(PyStr);
  expr_datum.set_string(common::ObString(result));
  Py_FinalizeEx();\

  return ret;
}

int  ObExprPythonUdf::cg_expr(ObExprCGCtx&  op_cg_ctx, const  ObRawExpr&  raw_expr, ObExpr&  rt_expr) const
{
  /*
  UNUSED(op_cg_ctx);
  int ret = OB_SUCCESS;
  CK(rt_expr.arg_cnt_ > 0);
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = &eval_python_udf;
  }
  return ret;
  */
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprPythonUdf::eval_python_udf;
  return  OB_SUCCESS;
}
}  // namespace sql
}  // namespace oceanbase