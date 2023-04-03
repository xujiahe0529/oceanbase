#define USING_LOG_PREFIX SQL_ENG

#define PY_SSIZE_T_CLEAN
#include <exception>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <iostream>
#include <fstream>

#include "lib/oblog/ob_log.h"

#include "share/object/ob_obj_cast.h"
#include "share/config/ob_server_config.h"
#include "share/datum/ob_datum_util.h"
#include "objit/common/ob_item_type.h"

#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"

#include "storage/ob_storage_util.h"

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
  type.set_int();
  //type.set_varchar();
  //type.set_length(512);
  //type.set_double();

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

int ObExprPythonUdf::get_python_udf(pythonUdf* &pyudf) 
{
  int ret = OB_SUCCESS;
  //初始化引擎
  pythonUdfEngine* udfEngine = pythonUdfEngine::init_python_udf_engine();
  //获取udf实例
  //std::string name = "simple*3";
  //std::string name = "difflib";
  std::string name = "iris";
  pythonUdf *udfPtr = nullptr;
  if(!udfEngine -> get_python_udf(name, udfPtr)) {
    //udf构造参数
    //char pycall[] = "import numpy as np\nimport pickle\ndef pyfun(x1, x2, x3, x4):\n\tmodel_path = '/home/test/model/iris_model.pkl'\n\twith open(model_path, 'rb') as m:\n\t\tmodel = pickle.load(m)\n\t\tx = np.column_stack((x1,x2,x3,x4))\n\t\ty = model.predict(x)\n\t\treturn y";
    char pycall[] = "import numpy as np\nimport pickle\ndef pyinitial():\n\tglobal model\n\tmodel_path = '/home/test/model/iris_model.pkl'\n\twith open(model_path, 'rb') as m:\n\t\tmodel = pickle.load(m)\n\ndef pyfun(x1, x2, x3, x4):\n\tx = np.column_stack((x1,x2,x3,x4))\n\ty = model.predict(x)\n\treturn y";
    
    //类型
    PyUdfSchema::PyUdfArgType *arg_list = new PyUdfSchema::PyUdfArgType[4];
    arg_list[0] = PyUdfSchema::PyUdfArgType::PyObj;
    arg_list[1] = PyUdfSchema::PyUdfArgType::PyObj;
    arg_list[2] = PyUdfSchema::PyUdfArgType::PyObj;
    arg_list[3] = PyUdfSchema::PyUdfArgType::PyObj;
    PyUdfSchema::PyUdfRetType *rt_type = new PyUdfSchema::PyUdfRetType[1]{PyUdfSchema::PyUdfRetType::PyObj};

    //初始化udf
    udfPtr = new pythonUdf();
    if(!udfPtr->init_python_udf(name, pycall, arg_list, 4, rt_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to init python udf", K(ret));
      return ret;
    }

    //插入udf_pool
    udfEngine->insert_python_udf(udfPtr->get_name(), udfPtr);
  }
  pyudf = udfPtr;
  //validation
  if(pyudf == NULL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get Python UDF", K(ret));
    return ret;
  }
  return ret;
}

int ObExprPythonUdf::eval_python_udf(const ObExpr& expr, ObEvalCtx& ctx, ObDatum&  expr_datum)
{
  int ret = OB_SUCCESS;

  //获取udf实例并核验
  pythonUdf *udfPtr = NULL;
  if(OB_FAIL(get_python_udf(udfPtr))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to obtain udf", K(ret));
    return ret;
  } else if(expr.arg_cnt_ != udfPtr->get_arg_count()){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong arg count", K(ret));
    return ret;
  }

  //设置udf运行参数
  ObDatum *argDatum = NULL;
  PyObject *numpyarray = NULL;
  for(int i = 0;i < udfPtr->get_arg_count();i++) {
    //get args from expr
    if(expr.args_[i]->eval(ctx, argDatum) != OB_SUCCESS){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to obtain arg", K(ret));
      return ret;
    }
    //转换得到numpy array --> 单一元素
    if(OB_FAIL(obdatum2array(argDatum, expr.args_[i]->datum_meta_.type_, numpyarray, 1))){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to obdatum2array", K(ret));
      return ret;
    }
    //插入pArg
    if(!udfPtr->set_arg_at(i, numpyarray)){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to set numpy array arg", K(ret));
      return ret;
    }
  }

  //执行Python Code
  if(!udfPtr->execute()){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("execute error", K(ret));
    return ret;
  }
  //获取返回值
  PyObject* result = NULL;
  if(!udfPtr->get_result(result)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("have not get result", K(ret));
    return ret;
  }

  //从numpy数组中取出返回值
  PyObject* value = nullptr;
  if(OB_FAIL(numpy2value(result, 0, value))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to obtain result value", K(ret));
    return ret;
  }
  
  //根据类型填入返回值
  switch (expr.datum_meta_.type_)
  {
    case ObCharType:
    case ObVarcharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: {
      expr_datum.set_string(common::ObString(PyBytes_AsString(value)));
      break;
    }
    case ObIntType: {
      //Py_SET_TYPE(value, &PyLong_Type);
      expr_datum.set_int(PyLong_AsLong(value));
      break;
    }
    case ObDoubleType:{
      expr_datum.set_double(PyFloat_AsDouble(value));
      break;
    }
    case ObNumberType: {
      //error
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not support ObNumberType", K(ret));
      return ret;
    }
    default: {
      //error
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown result type", K(ret));
      return ret;
    }
  }

  //释放资源
  PyArray_XDECREF((PyArrayObject *)numpyarray);
  PyArray_XDECREF((PyArrayObject *)result);
  //not need to DECREF value
  return ret;
}

int ObExprPythonUdf::eval_python_udf_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                    const ObBitVector &skip, const int64_t batch_size)
{
  LOG_DEBUG("eval python udf in batch mode", K(batch_size));
  int ret = OB_SUCCESS;

  /*
  //调整batch_size?
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(512);*/

  //返回值
  ObDatum *results = expr.locate_batch_datums(ctx);
  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr results frame is not init", K(ret));
  }

  //获取udf实例并核验
  pythonUdf *udfPtr = NULL;
  if(OB_FAIL(get_python_udf(udfPtr))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to obtain udf", K(ret));
    return ret;
  } else if(expr.arg_cnt_ != udfPtr->get_arg_count()){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong arg count", K(ret));
    return ret;
  }

  //获取参数datum数组
  ObDatum *arg_datums[expr.arg_cnt_];
  for(int i = 0;i < expr.arg_cnt_; i++) {
    arg_datums[i] = expr.args_[i]->locate_batch_datums(ctx);
  }

  //设置udf运行参数
  ObDatum *argDatum = NULL;
  PyObject *numpyarray = NULL;
  for(int i = 0;i < udfPtr->get_arg_count();i++) {
    //转换得到numpy array --> 多元素 --> batch size
    if(OB_FAIL(obdatum2array(arg_datums[i], expr.args_[i]->datum_meta_.type_, numpyarray, batch_size))){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to obdatum2array", K(ret));
      return ret;
    }
    //插入pArg
    if(!udfPtr->set_arg_at(i, numpyarray)){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to set numpy array arg", K(ret));
      return ret;
    }
  }

  //执行Python Code
  if(!udfPtr->execute()){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("execute error", K(ret));
    return ret;
  }

  //获取返回值
  PyObject* result = NULL;
  PyObject* value = NULL;
  if(!udfPtr->get_result(result)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("have not get result", K(ret));
    return ret;
  }

  //向上传递返回值
  for (int64_t j = 0; OB_SUCC(ret) && (j < batch_size); ++j) {
    //从numpy数组中取出返回值
    if(OB_FAIL(numpy2value(result, j, value))){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to obtain result value", K(ret));
      return ret;
    }
    //根据类型填入返回值
    switch (expr.datum_meta_.type_)
    {
      case ObCharType:
      case ObVarcharType:
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType: {
        results[j].set_string(common::ObString(PyBytes_AsString(value)));
        break;
      }
      case ObIntType: {
        //Py_SET_TYPE(value, &PyLong_Type);
        results[j].set_int(PyLong_AsLong(value));
        break;
      }
      case ObDoubleType:{
        results[j].set_double(PyFloat_AsDouble(value));
        break;
      }
      case ObNumberType: {
        //error
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not support ObNumberType", K(ret));
        break;
      }
      default: {
        //error
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown result type", K(ret));
        break;
      }
    }
  }

  //释放资源
  PyArray_XDECREF((PyArrayObject *)numpyarray);
  PyArray_XDECREF((PyArrayObject *)result);
  //not need to DECREF value

  return ret;
}


int ObExprPythonUdf::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  //绑定eval
  rt_expr.eval_func_ = ObExprPythonUdf::eval_python_udf;
  //绑定向量化eval
  bool is_batch = true;
  for(int i = 0; i < rt_expr.arg_cnt_; i++){
    if(!rt_expr.args_[i]->is_batch_result()) {
      is_batch = false;
      break;
    }
  }
  if(is_batch) {
    rt_expr.eval_batch_func_ = ObExprPythonUdf::eval_python_udf_batch;
  }
  return  OB_SUCCESS;
}

//PyAPI Methods
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


int ObExprPythonUdf::obdatum2array(ObDatum *&argdatum, const ObObjType &type, PyObject *&array, const int64_t batch_size) {
  _import_array(); //load numpy api
  int ret = OB_SUCCESS;
  npy_intp elements[1] = {batch_size};
  switch(type) {
    case ObCharType:
    case ObVarcharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: {
      //string in numpy array
      array = PyArray_New(&PyArray_Type, 1, elements, NPY_OBJECT, NULL, NULL, 0, 0, NULL);
      //set numpy data
      for(int i = 0; i < batch_size ; i++) {
        PyArray_SETITEM((PyArrayObject *)array, (char *)PyArray_GETPTR1((PyArrayObject *)array, i), 
          PyBytes_FromStringAndSize(argdatum[i].get_string().ptr(), argdatum[i].get_string().length()));
      }
      break;
    }
    case ObIntType: {
      //integer in numpy array
      array = PyArray_EMPTY(1, elements, NPY_INT32, 0);
      //set numpy data
      for(int i = 0; i < batch_size ; i++) {
        PyArray_SETITEM((PyArrayObject *)array, (char *)PyArray_GETPTR1((PyArrayObject *)array, i), PyLong_FromLong(argdatum[i].get_int()));
      }
      break;
    }
    case ObDoubleType: {
      //double in numpy array
      array = PyArray_EMPTY(1, elements, NPY_FLOAT64, 0);
      //set numpy data
      for(int i = 0; i < batch_size ; i++) {
        PyArray_SETITEM((PyArrayObject *)array, (char *)PyArray_GETPTR1((PyArrayObject *)array, i), PyFloat_FromDouble(argdatum[i].get_double()));
      }
      break;
    }
    case ObNumberType: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("number type, fail in obdatum2array", K(ret));
      return ret;
    }
    default: {
      //error
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown arg type, fail in obdatum2array", K(ret));
      return ret;
    }
  }
  return ret;
}


int ObExprPythonUdf::numpy2value(PyObject *numpyarray, const int loc, PyObject *&value) {
  _import_array(); //load numpy api
  int ret = OB_SUCCESS;
  try {
    value = PyArray_GETITEM((PyArrayObject *)numpyarray, (char *)PyArray_GETPTR1((PyArrayObject *)numpyarray, loc));
  } catch(const std::exception& e) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail in numpy2value", K(ret));
    return ret;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase