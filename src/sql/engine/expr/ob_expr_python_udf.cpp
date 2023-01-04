#define USING_LOG_PREFIX SQL_ENG

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <string.h>

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

ObExprPythonUdf::ObExprPythonUdf(ObIAllocator& alloc) : ObExprOperator(alloc, T_FUN_SYS_PYTHON_UDF, N_PYTHON_UDF, MORE_THAN_ZERO)
{}

ObExprPythonUdf::~ObExprPythonUdf()
{}

int ObExprPythonUdf::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types_array,
                                       int64_t param_num,
                                       common::ObExprTypeCtx &type_ctx) const 
{
  UNUSED(type_ctx);
  //need use ctx to decide type

  //type.set_int();
  //type.set_varchar();
  //type.set_length(512);
  type.set_double();
  //type.set_default_collation_type();
  //type.set_collation_level(CS_LEVEL_SYSCONST);

  return  OB_SUCCESS;
}

int ObExprPythonUdf::calc_resultN(common::ObObj &result,
                                  const common::ObObj *objs,
                                  int64_t param_num,
                                  common::ObExprCtx &expr_ctx) const
{
  UNUSED(expr_ctx);
  //result.set_varchar(common::ObString(SAY_HELLO));
  //result.set_collation(result_type_);
  return OB_SUCCESS;
}

int ObExprPythonUdf::eval_python_udf(const ObExpr& expr, ObEvalCtx& ctx, ObDatum&  expr_datum)
{
  int ret = OB_SUCCESS;
  Py_InitializeEx(Py_IsInitialized());
  //use Python Interpret
  PyObject *pModule, *pFunc, *dic, *v;
  PyObject *pArgs, *pResult;
  //load main module
  pModule = PyImport_AddModule("__main__");//0.085sec
  if (!pModule) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to add module", K(ret));
    return ret;
  }
  //add module 
  //PyRun_SimpleString("import difflib");

  //add UDF to the main module
  dic = PyModule_GetDict(pModule);
  /*
  //test i=i*2
  const char* pycall = "def pyfun(i):\n
                        \ti = i*2\n
                        \treturn i";
  //select python_udf(salary) from employee; --> 0.168sec
  */
  /*
  //test split
  //cannot use ' ', must use " "
  const char* pycall = "def pyfun(str):\n\
                        \tcut = str.split(" ")\n\
                        \treturn cut[0]";
  select python_udf(name) as first_name,salary from employee; --> 0.179sec
  */

  //test difflib
  //const char* pycall = "def pyfun(s1, s2):\n\
  //                      \treturn difflib.SequenceMatcher(None, s1, s2).ratio()";

  //obtain code
  ObDatum *codeDatum = NULL;
  common::ObString code;
  if(expr.args_[0]->eval(ctx, codeDatum) == OB_SUCCESS)
    code = codeDatum->get_string();
  if(code == NULL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to obtain pycode", K(ret));
    return ret;
  }
  int length = code.length();

  //obtain pycall
  char* pycall = new char[length];
  strcpy(pycall, code.ptr());
  if(pycall[length] == '\002' || pycall[length] == '\003')
    pycall[length] = '\0';

  //test pycall
  v = PyRun_StringFlags(pycall, Py_file_input, dic, dic, NULL);
  if(v == NULL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to run pycall", K(ret));
    return ret;
  }
  Py_DECREF(v);
  Py_DECREF(dic);

  //obtain function pointer called pFunc
  pFunc = PyObject_GetAttrString(pModule, "pyfun");//0.109sec
  if(!pFunc || !PyCallable_Check(pFunc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to obtain pFunc", K(ret));
    return ret;
  }
  
  //obtain arg_cnt
  ObDatum *argDatum = NULL;
  int argNum = -2;
  if(expr.args_[1]->eval(ctx, argDatum) == OB_SUCCESS)
    argNum = argDatum->get_int();
  if(expr.arg_cnt_ < argNum + 2){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uncorrect arg_cnt", K(ret));
    return ret;
  }
  
  //obtain args
  pArgs = PyTuple_New(argNum);
  for(int i = 0; i < argNum; i++){
    //get args from expr
    expr.args_[i+2]->eval(ctx, argDatum);
    //set pArgs according to its type
    switch(expr.args_[i+2]->datum_meta_.type_){
      case ObCharType:
      case ObVarcharType:
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType:
        PyTuple_SetItem(pArgs, i, PyBytes_FromStringAndSize(argDatum->get_string().ptr(),argDatum->get_string().length()));
        break;
      case ObIntType:
        PyTuple_SetItem(pArgs, i, PyLong_FromLong(argDatum->get_int()));
        break;
      case ObDoubleType:
        PyTuple_SetItem(pArgs, i, PyFloat_FromDouble(argDatum->get_double()));
        break;
      default:
        //error
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("uncorrect arg type", K(ret));
        return ret;
    }
  }
  
  //execute pFun
  pResult = PyObject_CallObject(pFunc, pArgs);
  Py_DECREF(pModule);
  Py_DECREF(pFunc);
  Py_DECREF(pArgs);
  
  //extract result
  if(pResult == NULL){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no result", K(ret));
    return ret;
  }
  else if(PyBytes_Check(pResult))
    expr_datum.set_string(common::ObString(PyBytes_AsString(pResult)));
  else if(PyByteArray_Check(pResult))
    expr_datum.set_string(common::ObString(PyByteArray_AsString(pResult)));
  else if(PyFloat_Check(pResult))
    expr_datum.set_double(PyFloat_AsDouble(pResult));
  else if(PyLong_Check(pResult))
    expr_datum.set_int(PyLong_AsLong(pResult));
  else if(PyUnicode_Check(pResult))
    expr_datum.set_string(common::ObString(PyUnicode_AsUTF8(pResult)));
  else
    throw new std::exception();
  
  Py_DECREF(pResult);
  Py_FinalizeEx();
  return ret;
}

int ObExprPythonUdf::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprPythonUdf::eval_python_udf;
  return  OB_SUCCESS;
}

//PyAPI Methods Unused
bool Python_ObtainGIL(void)
{
	PyGILState_STATE gstate = PyGILState_Ensure();
	return gstate == PyGILState_LOCKED ? 0 : 1;
}

bool Python_ReleaseGIL(bool state)
{
	PyGILState_STATE gstate =
		state == 0 ? PyGILState_LOCKED : PyGILState_UNLOCKED;
	PyGILState_Release(gstate);
	return 0;
}

}  // namespace sql
}  // namespace oceanbase