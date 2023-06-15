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
#include "python_udf_util.h"
#include <sys/time.h>
#include <frameobject.h>
#include <fstream>

#include "share/datum/ob_datum_util.h"
#endif

namespace oceanbase
{
    namespace sql 
    {
        //---------------------------------OB Result ----------------------------------
        class ObResult {
        public:
            ObDatum *ptrHead; // 指针头
            int *index; // 定位数组
            int size; //元组个数
            ObResult *next; //迭代

            ObResult();
            ~ObResult();
            //set 方法
            bool init(ObDatum* resultHead, int length);
            bool setIndex(int loc, int pos);
            int getIndex(int loc);
            bool setNext(ObResult* Next);
        };

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

            //包装参数的numpy Array
            PyObject** numpyArrays;

            //向量化参数
            int batch_size, currentIndex;

            //指向ObResult头
            ObResult *ObResultArray;
            //指向当前ObResult
            ObResult *currentResult;
            
        public:
            pythonUdf();
            ~pythonUdf();
            std::string get_name();
            bool init_python_udf(std::string name, char* pycall, PyUdfSchema::PyUdfArgType* arg_list, int length, PyUdfSchema::PyUdfRetType* rt_type);
            int get_arg_count();
            //设置参数->重载
            bool set_arg_at(int i, long const& arg);
            bool set_arg_at(int i, double const& arg);
            bool set_arg_at(int i, bool const& arg);
            bool set_arg_at(int i, std::string const& arg);
            bool set_arg_at(int i, PyObject* arg);

            //探测余量
            int detectCapacity(int size);
            //移动下标
            bool moveIndex(int size);
            //插入numpy array
            bool insertNumpyArray(int i, int j, char* ptr, long length);
            bool insertNumpyArray(int i, int j, long const& arg);
            bool insertNumpyArray(int i, int j, double const& arg);
            //execute
            bool executeNumpyArrays();
            //reset
            bool resetNumpyArrays(int newBatchSize);

            //初始化及执行
            bool execute_initial();
            bool execute();
            //获取执行结果->重载
            bool get_result(long& result);
            bool get_result(double& result);
            bool get_result(bool& result);
            bool get_result(std::string& result);
            bool get_result(PyObject*& result);
            
            //加入新的ObResult
            bool addNewObResult(ObDatum* resultHead, int length);
            //插入映射关系，对应ObResult::setIndex
            bool insertObResult(int loc, int pos);
            //获取映射关系，对应ObResult::getIndex
            int getIndexObResult(int loc);
            //返回buffer计算好的值
            bool returnObResult();
            //重置ObResult
            bool resetObResult();
            //算子结束信号
            bool endOperatorSignal();

            //异常处理
            void process_python_exception();
            void message_error_dialog_show(char *buf);
            //测量计时
            timeval *tv;
            
            //测试result
            ObDatum *resultptr;
            void setptr();
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

            bool endAll();//结束所有UDF的等待过程，进行提交
        };
    }
}