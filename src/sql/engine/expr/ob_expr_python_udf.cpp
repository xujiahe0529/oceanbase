#define USING_LOG_PREFIX SQL_ENG

#define PY_SSIZE_T_CLEAN
#include <exception>
#include <string.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <iostream>
#include <fstream>
#include <sstream>

#include "lib/oblog/ob_log.h"

#include "share/object/ob_obj_cast.h"
#include "share/config/ob_server_config.h"
#include "share/datum/ob_datum_util.h"
#include "objit/common/ob_item_type.h"

#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"

#include "storage/ob_storage_util.h"
#include "sql/engine/expr/ob_expr_python_udf.h"

static const char* SAY_HELLO = "This is Python UDF.";
static const char* file = "/etc/buf";
static const char *fileName = "/home/test/log/time_transform_log";

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
  //type.set_double();
  //type.set_varchar();
  //type.set_length(512);
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

int ObExprPythonUdf::get_python_udf(pythonUdf* &pyudf, const ObExpr& expr) 
{
  int ret = OB_SUCCESS;
  //初始化引擎
  pythonUdfEngine* udfEngine = pythonUdfEngine::init_python_udf_engine();
  //获取udf实例
  std::string name = "expedia";
  pythonUdf *udfPtr = nullptr;
  if(!udfEngine -> get_python_udf(name, udfPtr)) {
    //udf构造参数
    //char pycall[] = "import numpy as np\nimport pickle\ndef pyinitial():\n\tglobal model\n\tmodel_path = '/home/test/model/iris_model.pkl'\n\twith open(model_path, 'rb') as m:\n\t\tmodel = pickle.load(m)\ndef pyfun(*args):\n\tx = np.column_stack(args)\n\ty = model.predict(x)\n\treturn y";
    //char pycall[] = "import pickle\nimport numpy as np\nimport pandas as pd\nfrom sklearn.preprocessing import OneHotEncoder\nfrom sklearn.preprocessing import StandardScaler\ndef pyinitial(): pass\ndef pyfun(*args):\n\tscaler_path = '/home/test/model/expedia_standard_scale_model.pkl'\n\tenc_path = '/home/test/model/expedia_one_hot_encoder.pkl'\n\tmodel_path = '/home/test/model/expedia_lr_model.pkl'\n\twith open(scaler_path, 'rb') as f:\n\t\tscaler = pickle.load(f)\n\twith open(enc_path, 'rb') as f:\n\t\tenc = pickle.load(f)\n\twith open(model_path, 'rb') as f:\n\t\tmodel = pickle.load(f)\n\tdata = np.column_stack(args)\n\tdata = np.split(data, np.array([8]), axis = 1)\n\tnumerical = data[0]\n\tcategorical = data[1]\n\tX = np.hstack((scaler.transform(numerical), enc.transform(categorical).toarray()))\n\treturn model.predict(X)";
    char pycall[] = "import pickle\nimport numpy as np\nimport pandas as pd\nfrom sklearn.preprocessing import OneHotEncoder\nfrom sklearn.preprocessing import StandardScaler\ndef pyinitial():\n\tglobal scaler, enc, model\n\tscaler_path = '/home/test/model/expedia_standard_scale_model.pkl'\n\tenc_path = '/home/test/model/expedia_one_hot_encoder.pkl'\n\tmodel_path = '/home/test/model/expedia_lr_model.pkl'\n\twith open(scaler_path, 'rb') as f:\n\t\tscaler = pickle.load(f)\n\twith open(enc_path, 'rb') as f:\n\t\tenc = pickle.load(f)\n\twith open(model_path, 'rb') as f:\n\t\tmodel = pickle.load(f)\ndef pyfun(*args):\n\tdata = np.column_stack(args)\n\tdata = np.split(data, np.array([8]), axis = 1)\n\tnumerical = data[0]\n\tcategorical = data[1]\n\tX = np.hstack((scaler.transform(numerical), enc.transform(categorical).toarray()))\n\treturn model.predict(X)";
    /*char pycall[] = "import pickle\
\nimport time\
\nimport numpy as np\
\nimport pandas as pd\
\nfrom sklearn.preprocessing import OneHotEncoder\
\nfrom sklearn.preprocessing import StandardScaler\
\ndef pyinitial():\
\n\tglobal preprocess, process\
\n\tpreprocess = 0\
\n\tprocess = 0\
\n\tglobal scaler, enc, model\
\n\tscaler_path = '/home/test/model/expedia_standard_scale_model.pkl'\
\n\tenc_path = '/home/test/model/expedia_one_hot_encoder.pkl'\
\n\tmodel_path = '/home/test/model/expedia_lr_model.pkl'\
\n\twith open(scaler_path, 'rb') as f:\
\n\t\tscaler = pickle.load(f)\
\n\twith open(enc_path, 'rb') as f:\
\n\t\tenc = pickle.load(f)\
\n\twith open(model_path, 'rb') as f:\
\n\t\tmodel = pickle.load(f)\
\ndef pyfun(*args):\
\n\tglobal preprocess, process\
\n\tstart = time.time()\
\n\tdata = np.column_stack(args)\
\n\tdata = np.split(data, np.array([8]), axis = 1)\
\n\tnumerical = data[0]\
\n\tcategorical = data[1]\
\n\tX = np.hstack((scaler.transform(numerical), enc.transform(categorical).toarray()))\
\n\tend = time.time()\
\n\tpreprocess += end - start\
\n\tstart = time.time()\
\n\ty = model.predict(X)\
\n\tend = time.time()\
\n\tprocess += end - start\
\n\ttime_path = '/home/test/log/expedia/runtime'\
\n\twith open(time_path, 'w') as f:\
\n\t\tf.write(str(preprocess))\
\n\t\tf.write('\\n')
\n\t\tf.write(str(process))\
\n\treturn y";*/
    /*char pycall[] = "import pickle\
    \nimport numpy as np\nimport pandas as pd\
    \nfrom sklearn.preprocessing import OneHotEncoder\
    \nfrom sklearn.preprocessing import StandardScaler\
    \ndef pyinitial():\n\tglobal scaler, enc, model\
    \n\tscaler_path = '/home/test/model/expedia_standard_scale_model.pkl'\
    \n\tenc_path = '/home/test/model/expedia_one_hot_encoder.pkl'\
    \n\tmodel_path = '/home/test/model/expedia_lr_model.pkl'\
    \n\twith open(scaler_path, 'rb') as f:\
    \n\t\tscaler = pickle.load(f)\
    \n\twith open(enc_path, 'rb') as f:\
    \n\t\tenc = pickle.load(f)\
    \n\twith open(model_path, 'rb') as f:\
    \n\t\tmodel = pickle.load(f)\
    \ndef pyfun(*args):\
    \n\tdata = np.column_stack(args)\
    \n\tfile_path = '/home/test/log/expedia/exception.csv'\
    \n\tnp.savetxt(file_path, data, fmt = '%s', delimiter=',')\
    \n\treturn args[10]";*/
    
    //类型
    int size = expr.arg_cnt_;
    //int size = 28;
    //PyUdfSchema::PyUdfArgType *arg_list = new PyUdfSchema::PyUdfArgType[size];
    //根据ObObjtype判断是什么类型
    PyUdfSchema::PyUdfArgType *arg_list = new PyUdfSchema::PyUdfArgType[expr.arg_cnt_];
    for(int i = 0; i < expr.arg_cnt_; i++) {
      switch(expr.args_[i]->datum_meta_.type_) {
        case ObCharType:
        case ObVarcharType:
        case ObTinyTextType:
        case ObTextType:
        case ObMediumTextType:
        case ObLongTextType: {
            arg_list[i] = PyUdfSchema::PyUdfArgType::STRING;
            break;
        }
        case ObTinyIntType:
        case ObSmallIntType:
        case ObMediumIntType:
        case ObInt32Type:
        case ObIntType: {
            arg_list[i] = PyUdfSchema::PyUdfArgType::INTEGER;
            break;
        }
        case ObDoubleType: {
            arg_list[i] = PyUdfSchema::PyUdfArgType::DOUBLE;
            break;
        }
        case ObNumberType: {
            return false;
            break;
        }
        default: {
            //error
            return false;
        }
      }
    }
  
    /*手动定义太麻烦
    for(int i = 0; i < 8; i ++)
      arg_list[i] = PyUdfSchema::PyUdfArgType::DOUBLE;
    arg_list[5] = PyUdfSchema::PyUdfArgType::INTEGER;
    arg_list[6] = PyUdfSchema::PyUdfArgType::DOUBLE;
    */
    PyUdfSchema::PyUdfRetType *rt_type = new PyUdfSchema::PyUdfRetType[1]{PyUdfSchema::PyUdfRetType::LONG};

    //初始化udf
    udfPtr = new pythonUdf();
    if(!udfPtr->init_python_udf(name, pycall, arg_list, size, rt_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to init python udf", K(ret));
      return ret;
    }

    //插入udf_pool
    udfEngine->insert_python_udf(udfPtr->get_name(), udfPtr);
  }
  pyudf = udfPtr;
  pyudf->resultptr = nullptr;
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
  if(OB_FAIL(get_python_udf(udfPtr, expr))){
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
      expr_datum.set_string(common::ObString(PyUnicode_AS_DATA(value)));
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
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
  
  //struct timeval t1, t2, t3, t4, tsub;
  //record start time
  //gettimeofday(&t1, NULL);

  LOG_DEBUG("eval python udf in batch mode", K(batch_size));
  int ret = OB_SUCCESS;

  //返回值
  ObDatum *results = expr.locate_batch_datums(ctx);

  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr results frame is not init", K(ret));
    return ret;
  }

  //获取udf实例并核验
  pythonUdf *udfPtr = NULL;
  if(OB_FAIL(get_python_udf(udfPtr, expr))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to obtain udf", K(ret));
    return ret;
  } else if (expr.arg_cnt_ != udfPtr->get_arg_count()){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong arg count", K(ret));
    return ret;
  }

  //eval and check params
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObBitVector &my_skip = expr.get_pvt_skip(ctx);
  my_skip.deep_copy(skip, batch_size);
  for (int i = 0; i < udfPtr->get_arg_count(); i++) {
    //do eval
    if (OB_FAIL(expr.args_[i]->eval_batch(ctx, my_skip, batch_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to eval batch result args", K(ret));
      return ret;
    }
    //do check
    ObDatum *datum_array = expr.args_[i]->locate_batch_datums(ctx);
    for (int j = 0; j < batch_size; j++) {
      if (my_skip.at(j) || eval_flags.at(j))
        continue;
      else if (datum_array[j].is_null()) {
        //存在null推理结果即为空
        results[j].set_null();
        my_skip.set(j);
        eval_flags.set(j);
      }
    }
  }
  //真实batch_size
  const int64_t real_param = batch_size - my_skip.accumulate_bit_cnt(batch_size);

  //设置udf运行参数
  
  //gettimeofday(&t1, NULL);
  _import_array(); //load numpy api

  PyObject** arrays = new PyObject*[udfPtr->get_arg_count()];

  //int* intptr = new int[real_param];
  //转换得到numpy array
  for (int i = 0; i < udfPtr->get_arg_count(); i++) {
    
    //获取参数datum vector
    ObDatumVector param_datums = expr.args_[i]->locate_expr_datumvector(ctx);
    ObDatum* argDatum = NULL;
    npy_intp elements[1] = {real_param};
    PyObject *numpyarray = NULL;
    int j = 0; //size of array <= batch_size
    ObObjType type = expr.args_[i]->datum_meta_.type_;
    switch(type) {
      case ObCharType:
      case ObVarcharType:
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType: {
        //string in numpy array
        numpyarray = PyArray_New(&PyArray_Type, 1, elements, NPY_OBJECT, NULL, NULL, 0, 0, NULL);
        //set numpy data
        for (int k = 0; k < batch_size ; k++) {
          if (skip.at(k) || eval_flags.at(k)) {
            continue;
          } else {
            argDatum = param_datums.at(k);
            ObString str = argDatum->get_string();
            PyArray_SETITEM((PyArrayObject *)numpyarray, (char *)PyArray_GETPTR1((PyArrayObject *)numpyarray, j++), 
              PyUnicode_FromStringAndSize(str.ptr(), str.length()));
          }
        }
        break;
      }
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType: {
        //integer in numpy array
        numpyarray = PyArray_EMPTY(1, elements, NPY_INT32, 0);
        //set numpy data
        for(int k = 0; k < batch_size ; k++) {
          if (skip.at(k) || eval_flags.at(k)) {
            continue;
          } else {
            argDatum = param_datums.at(k);
            PyArray_SETITEM((PyArrayObject *)numpyarray, (char *)PyArray_GETPTR1((PyArrayObject *)numpyarray, j++), PyLong_FromLong(argDatum->get_int()));
          }
        }
        break;
      }
      case ObDoubleType: {
        //double in numpy array
        numpyarray = PyArray_EMPTY(1, elements, NPY_FLOAT64, 0);
        //set numpy data
        for(int k = 0; k < batch_size ; k++) {
          if (skip.at(k) || eval_flags.at(k)) {
            continue;
          } else {
            argDatum = param_datums.at(k);
            PyArray_SETITEM((PyArrayObject *)numpyarray, (char *)PyArray_GETPTR1((PyArrayObject *)numpyarray, j++), PyFloat_FromDouble(argDatum->get_double()));
          }
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
    // check array size
    if(j != real_param) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("incorrect array size", K(ret));
      return ret;
    }

    /*
    //转换得到numpy array --> 多元素 --> batch size
    if(OB_FAIL(obdatum2array(param_datums, expr.args_[i]->datum_meta_.type_, numpyarray, batch_size, real_param, data, my_skip, eval_flags))){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to obdatum2array", K(ret));
      return ret;
    }*/

    //插入pArg
    if(!udfPtr->set_arg_at(i, numpyarray)){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to set numpy array arg", K(ret));
      return ret;
    }
    //
    arrays[i] = numpyarray;
  }
  //gettimeofday(&t2, NULL);
  //执行Python Code
  if(!udfPtr->execute()){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("execute error", K(ret));
    return ret;
  }
  //gettimeofday(&t3, NULL);
  //获取返回值
  PyObject* result = NULL;
  if(!udfPtr->get_result(result)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("have not get result", K(ret));
    return ret;
  }

  //向上传递返回值
  int k = 0; // loc of value
  PyObject* value = NULL;
  //根据类型填入返回值
  switch (expr.datum_meta_.type_)
  {
    case ObCharType:
    case ObVarcharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: {
      for (int64_t j = 0; OB_SUCC(ret) && (j < batch_size); ++j) {
        //去重
        if (skip.at(j) || eval_flags.at(j)) 
          continue;
        else {
          numpy2value(result, k++, value);
          results[j].set_string(common::ObString(PyUnicode_AS_DATA(value)));
        }
      }
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      /*
      int* numptr = NULL;
      PyArray_AsCArray(result, numptr, elements, 1, NULL);*/
      for (int64_t j = 0; OB_SUCC(ret) && (j < batch_size); ++j) {
        //去重
        if (skip.at(j) || eval_flags.at(j)) 
          continue;
        else {
          numpy2value(result, k++, value);
          results[j].set_int(PyLong_AsLong(value));
        }
      }
      break;
    }
    case ObDoubleType:{
      for (int64_t j = 0; OB_SUCC(ret) && (j < batch_size); ++j) {
        //去重
        if (skip.at(j) || eval_flags.at(j)) 
          continue;
        else {
          numpy2value(result, k++, value);
          results[j].set_double(PyFloat_AsDouble(value));
        }
      }
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

  //gettimeofday(&t4, NULL);
  //释放资源
  for (int i = 0; i < udfPtr->get_arg_count(); i++) {
    PyArray_XDECREF((PyArrayObject *)arrays[i]);
  }
  delete[] arrays;
  PyArray_XDECREF((PyArrayObject *)result);
  //not need to DECREF value
  

  /*std::ofstream out;
  out.open(fileName, std::ios::app);
  if(out.is_open()){
    
    //out << "batch: " << i++  << std::endl;
    out << "batch size: " << batch_size  << std::endl;
    out << "real batch size: " << real_param  << std::endl << std::endl;
  
    double tu;
    timersub(&t2, &t1, &tsub);
    tu = tsub.tv_sec*1000 + (1.0 * tsub.tv_usec)/1000;

    out << "time of transform: " << tu << " ms" << std::endl;

    timersub(&t3, &t2, &tsub);
    tu = tsub.tv_sec*1000 + (1.0 * tsub.tv_usec)/1000;
    out << "time of execute: " << tu << " ms"  << std::endl;

    timersub(&t4, &t3, &tsub);
    tu = tsub.tv_sec*1000 + (1.0 * tsub.tv_usec)/1000;
    out << "time of update: " << tu << " ms"  << std::endl << std::endl;
     
    out.close();
  }*/
 
  return ret;
}

int ObExprPythonUdf::eval_python_udf_batch_buffer(const ObExpr &expr, ObEvalCtx &ctx,
                                    const ObBitVector &skip, const int64_t batch_size)
{
  LOG_DEBUG("eval python udf in batch buffer mode", K(batch_size));
  int ret = OB_SUCCESS;

  //返回值指针
  ObDatum *results = expr.locate_batch_datums(ctx);
  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr results frame is not init", K(ret));
  }

  //获取udf实例并核验
  pythonUdf *udfPtr = NULL;
  if(OB_FAIL(get_python_udf(udfPtr, expr))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to obtain udf", K(ret));
    return ret;
  } else if (expr.arg_cnt_ != udfPtr->get_arg_count()){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong arg count", K(ret));
    return ret;
  }

  //eval and check params
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObBitVector &my_skip = expr.get_pvt_skip(ctx);
  my_skip.deep_copy(skip, batch_size);
  for (int i = 0; i < udfPtr->get_arg_count(); i++) {
    //do eval
    if (OB_FAIL(expr.args_[i]->eval_batch(ctx, my_skip, batch_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to eval batch result args", K(ret));
      return ret;
    }
    //do check
    ObDatum *datum_array = expr.args_[i]->locate_batch_datums(ctx);
    for (int j = 0; j < batch_size; j++) {
      if (my_skip.at(j) || eval_flags.at(j))
        continue;
      else if (datum_array[j].is_null()) {
        //存在null推理结果即为空
        results[j].set_null();
        my_skip.set(j);
        eval_flags.set(j);
      }
    }
  }
  //真实batch_size
  const int64_t real_param = batch_size - my_skip.accumulate_bit_cnt(batch_size);

  _import_array(); //load numpy api

  int readyRows = real_param;
  int i, j, k;
  j = 0;
  do {
    //探测余量
    int bufferCapacity = udfPtr->detectCapacity(readyRows);
    readyRows -= bufferCapacity;
    //插入ObResult指针数组
    udfPtr->addNewObResult(results, bufferCapacity);
    //填充ObResult, i:loc, j:pos
    for(i = 0; j < batch_size && i < bufferCapacity; j++) {
      if(!my_skip.at(j) && !eval_flags.at(j)) 
        udfPtr->insertObResult(i++, j);
    }
    //行列不同的取参数过程是否会对性能造成影响？
    //填充numpy Array, i:列号, k: loc
    for (i = 0; i < udfPtr->get_arg_count(); i++) {
      //获取参数datum vector
      ObDatumVector param_datums = expr.args_[i]->locate_expr_datumvector(ctx);
      ObDatum* argDatum = NULL;
      switch(expr.args_[i]->datum_meta_.type_) {
        case ObCharType:
        case ObVarcharType:
        case ObTinyTextType:
        case ObTextType:
        case ObMediumTextType:
        case ObLongTextType: {
          //unicode
          for (k = 0; k < bufferCapacity ;k++) {
            int pos = udfPtr->getIndexObResult(k);
            argDatum = param_datums.at(pos);
            ObString str = argDatum->get_string();
            udfPtr->insertNumpyArray(i, k, str.ptr(), str.length());
          }
          break;
        }
        case ObTinyIntType:
        case ObSmallIntType:
        case ObMediumIntType:
        case ObInt32Type:
        case ObIntType: {
          //int
          for (k = 0; k < bufferCapacity ; k++) {
            int pos = udfPtr->getIndexObResult(k);
            argDatum = param_datums.at(pos);
            udfPtr->insertNumpyArray(i, k, argDatum->get_int());
          }
          break;
        }
        case ObDoubleType: {
          //double
          for (k = 0; k < bufferCapacity ; k++) {
            int pos = udfPtr->getIndexObResult(k);
            argDatum = param_datums.at(pos);
            udfPtr->insertNumpyArray(i, k, argDatum->get_double());
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
    }
    //移动下标
    udfPtr->moveIndex(bufferCapacity);
  } while (readyRows != 0);

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

//for one tuple
int ObExprPythonUdf::obdatum2array(const ObDatum *argdatum, const ObObjType &type, PyObject *&array, const int64_t batch_size) {
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
        ObString str = argdatum[i].get_string();
        PyArray_SETITEM((PyArrayObject *)array, (char *)PyArray_GETPTR1((PyArrayObject *)array, i), 
          PyUnicode_FromStringAndSize(str.ptr(), str.length()));
      }
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
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

//for vectorization
int ObExprPythonUdf::obdatum2array(const ObDatumVector &vector, const ObObjType &type, PyObject *&array, 
                                   const int64_t batch_size, const int64_t real_param,  
                                   const ObBitVector &skip, ObBitVector &eval_flags) {
  //_import_array(); //load numpy api
  int ret = OB_SUCCESS;
  npy_intp elements[1] = {real_param};
  ObDatum* argDatum = NULL;
  int j = 0; //size of array <= batch_size
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
      for (int i = 0; i < batch_size ; i++) {
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        } else {
          argDatum = vector.at(i);
          ObString str = argDatum->get_string();
          PyArray_SETITEM((PyArrayObject *)array, (char *)PyArray_GETPTR1((PyArrayObject *)array, j++), 
            PyUnicode_FromStringAndSize(str.ptr(), str.length()));
        }
      }
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      //integer in numpy array
      array = PyArray_EMPTY(1, elements, NPY_INT32, 0);
      //set numpy data
      for(int i = 0; i < batch_size ; i++) {
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        } else {
          argDatum = vector.at(i);
          PyArray_SETITEM((PyArrayObject *)array, (char *)PyArray_GETPTR1((PyArrayObject *)array, j++), PyLong_FromLong(argDatum->get_int()));
        }
      }
      break;
    }
    case ObDoubleType: {
      //double in numpy array
      array = PyArray_EMPTY(1, elements, NPY_FLOAT64, 0);
      //set numpy data
      for(int i = 0; i < batch_size ; i++) {
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        } else {
          argDatum = vector.at(i);
          PyArray_SETITEM((PyArrayObject *)array, (char *)PyArray_GETPTR1((PyArrayObject *)array, j++), PyFloat_FromDouble(argDatum->get_double()));
        }
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
  // check array size
  if(j != real_param) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incorrect array size", K(ret));
    return ret;
  }
  return ret;
}

int ObExprPythonUdf::numpy2value(PyObject *numpyarray, const int loc, PyObject *&value) {
  //_import_array(); //load numpy api
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