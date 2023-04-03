#define PY_SSIZE_T_CLEAN
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

#ifndef _PYTHON_UDF_ENGINE_H_
#define _PYTHON_UDF_ENGINE_H_
#include <Python.h>
#include <numpy/arrayobject.h>
#include <numpy/npy_common.h>
#include <map>
#include <string>
#include <exception>
#include "python_udf_schema.h"
#include <sys/time.h>
#endif

namespace oceanbase
{
    namespace sql 
    {
        //------------------------------------Udf------------------------------------
        class pythonUdf {
            private:
                char* pycall;//code
                std::string name;
                int arg_count;//参数数量
                //参数类型
                PyUdfSchema::PyUdfArgType* arg_types;
                //返回值类型,多返回值?
                bool mul_rt;
                PyUdfSchema::PyUdfRetType* rt_type;
                //运行变量
                PyObject *pArgs,*pResult;
                //初始化变量
                PyObject *pModule, *pFunc, *pInitial, *dic, *v;

                //向量化参数
                int batch_size;
                
            public:
                pythonUdf();
                ~pythonUdf();
                std::string get_name();
                bool init_python_udf(std::string name, char* pycall, PyUdfSchema::PyUdfArgType* arg_list, int length, PyUdfSchema::PyUdfRetType* rt_type);
                int get_arg_count();
                void reset_args();
                //设置参数->重载
                bool set_arg_at(int i, long const& arg);
                bool set_arg_at(int i, double const& arg);
                bool set_arg_at(int i, bool const& arg);
                bool set_arg_at(int i, std::string const& arg);
                bool set_arg_at(int i, PyObject* arg);
                //初始化及执行
                bool execute_initial();
                bool execute();
                //获取执行结果->重载
                bool get_result(long& result);
                bool get_result(double& result);
                bool get_result(bool& result);
                bool get_result(std::string& result);
                bool get_result(PyObject*& result);

                //测量计时
                timeval *tv;
                
        };

        //互斥锁
        //pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

        //------------------------------------Engine------------------------------------
        //单例模式
        class pythonUdfEngine {
        private:
            //key-value map,存储已经初始化的python_udf
            std::map<std::string, pythonUdf*> udf_pool;
            //只能有一个实例存在
            static pythonUdfEngine *current_engine;
            
        public:
            pythonUdfEngine(/* args */);
            ~pythonUdfEngine();
            //懒汉模式
            static pythonUdfEngine* init_python_udf_engine() {
                if(current_engine == NULL) {
                    //pthread_mutex_lock(&mutex);
                    current_engine = new pythonUdfEngine();
                    //pthread_mutex_unlock(&mutex);
                }
                return current_engine;
            }
            bool insert_python_udf(std::string name, pythonUdf *udf);
            bool get_python_udf(std::string name, pythonUdf *& udf);//获取udf，只能有一个同名实例存在
            bool show_names(std::string* names);//获取全部udf的名称

            
        };
    }
}