#include "python_udf_engine.h"


static const char *fileName = "/home/test/log/batch_buffer_log";

namespace oceanbase
{
    namespace sql 
    {
        //------------------------------------Engine 方法------------------------------------

        //定义静态变量
        pythonUdfEngine* pythonUdfEngine::current_engine = NULL;

        pythonUdfEngine::pythonUdfEngine(/* args */) {
            Py_InitializeEx(Py_IsInitialized());
        }

        pythonUdfEngine::~pythonUdfEngine() {
            //用迭代器销毁udf_pool中所有实例
            for(auto iter = udf_pool.begin(); iter != udf_pool.end(); iter ++){
                delete iter->second;
                iter->second = nullptr;
            }
            udf_pool.clear();
            //销毁当前engine
            Py_FinalizeEx();
            pythonUdfEngine* current_engine = nullptr;
        }

        //填入udf
        bool pythonUdfEngine::insert_python_udf(std::string name, pythonUdf *udf) {
            return udf_pool.insert(std::pair<std::string, pythonUdf*>(name, udf)).second;
        }
        //从udf_pool中获取已生成udf
        bool pythonUdfEngine::get_python_udf(std::string name, pythonUdf *& udf) {
            auto iter = udf_pool.find(name);
            if(iter != udf_pool.end()){
                udf = iter->second;
                return true;
            } else{
                //从存储中获取udf metadata 并构建udf实例

                //没有该udf名
            }
            return false;
        }
        
        //udf_pool层面的end
        bool pythonUdfEngine::endAll() {
            for(auto iter = udf_pool.begin(); iter != udf_pool.end(); iter ++){
                iter->second->endOperatorSignal();
            }
            return true;
        }

        //------------------------------------UDF 方法------------------------------------
        pythonUdf::pythonUdf(/* args */) {
            pycall = nullptr;
            name = "uninitalized_udf";
            arg_count = 0;
            arg_types = nullptr;
            rt_type = nullptr;
            batch_size = 4096; //default
            currentIndex = 0;
            //test time
            tv = nullptr;
            //test ptr
            resultptr = nullptr;
            numpyArrays = nullptr;
            ObResultArray = nullptr;
            currentResult = nullptr;
        }

        pythonUdf::~pythonUdf() {
            //释放内存
            name.clear();
            delete pycall;
            pycall = nullptr;
            delete arg_types;
            arg_types = nullptr;
            delete rt_type;
            rt_type = nullptr;
            delete tv;
            tv = nullptr;

            //resultptr = nullptr;
            currentResult = nullptr;
            delete ObResultArray;
            ObResultArray = nullptr;

            //释放Python解释器计数
            Py_XDECREF(pModule);
            Py_XDECREF(pFunc);
            Py_XDECREF(dic);
            Py_XDECREF(v);
            Py_XDECREF(pArgs);
            Py_XDECREF(pResult);
            Py_XDECREF(pInitial);

            //释放指针数组
            for (int i = 0; i < arg_count; i++) {
                PyArray_XDECREF((PyArrayObject *)numpyArrays[i]);
            }
            delete[] numpyArrays;
            numpyArrays = nullptr;
        }

        //初始化udf
        bool pythonUdf::init_python_udf(std::string name, char* pycall, PyUdfSchema::PyUdfArgType* arg_list, int length, PyUdfSchema::PyUdfRetType* rt_type) {
            try{
                _import_array(); //load numpy api

                tv = new timeval();
                gettimeofday(tv, NULL);
                this->name = name;
                //利用某种方式获取udf元信息 即代码、参数数量、参数类型和返回值类型
                this->pycall = pycall;
                //设置参数类型和返回值类型
                this->arg_count = length;
                this->arg_types = arg_list;
                this->rt_type = rt_type;
                //load main module
                pModule = PyImport_AddModule("__main__");
                if(!pModule)
                    return false;
                //add UDF to the main module
                dic = PyModule_GetDict(pModule);
                if(!dic)
                    return false;
                //test pycall
                v = PyRun_StringFlags(pycall, Py_file_input, dic, dic, NULL);
                if(!v)
                    return false;
                //obtain function pointer called pFunc
                pFunc = PyObject_GetAttrString(pModule, "pyfun");
                if(!pFunc || !PyCallable_Check(pFunc))
                    return false;
                //obtain function pointer initialization
                pInitial = PyObject_GetAttrString(pModule, "pyinitial");
                if(!pInitial || !PyCallable_Check(pInitial))
                    return false;
                //初始化pArgs
                pArgs = PyTuple_New(arg_count);
                if(!pArgs)
                    return false;
                //初始化numpy参数数组
                numpyArrays = new PyObject* [arg_count];
                npy_intp numpySize[1] = {batch_size};
                for (int i = 0; i < arg_count; i++) {
                    switch(arg_types[i]) {
                        case PyUdfSchema::PyUdfArgType::INTEGER: { 
                            numpyArrays[i] = PyArray_EMPTY(1, numpySize, NPY_INT32, 0);
                            break;
                        }
                        case PyUdfSchema::PyUdfArgType::DOUBLE: {
                            numpyArrays[i] = PyArray_EMPTY(1, numpySize, NPY_FLOAT64, 0);
                            break;
                        }
                        case PyUdfSchema::PyUdfArgType::STRING: {
                            numpyArrays[i] = PyArray_New(&PyArray_Type, 1, numpySize, NPY_OBJECT, NULL, NULL, 0, 0, NULL);
                            break;
                        }
                        default: {
                            return false;
                            break;
                        }
                    }
                }
                //初始化指针定位器，空链表头
                ObResultArray = new ObResult();
                currentResult = ObResultArray;
                resultptr = nullptr;
                return execute_initial();
            } catch(std::exception &e) {
                return false;
            }
        }
        //获取名字
        std::string pythonUdf::get_name() {
            return name;
        }

        //获取参数数量
        int pythonUdf::get_arg_count() {
            return arg_count;
        }

        //设置参数->重载
        bool pythonUdf::set_arg_at(int i, long const& arg) {
            if((i >= arg_count) | (arg_types[i] != PyUdfSchema::PyUdfArgType::INTEGER)) {
                return false;
            }
            try {
                PyTuple_SetItem(pArgs, i, PyLong_FromLong(arg));
            } catch(std::exception &e) {
                return false;
            }
            return true;
        }
        bool pythonUdf::set_arg_at(int i, bool const& arg) {
            if((i >= arg_count) | (arg_types[i] != PyUdfSchema::PyUdfArgType::BOOLEAN)) {
                return false;
            }
            try {
                if(arg)
                    PyTuple_SetItem(pArgs, i, Py_True);
                else
                    PyTuple_SetItem(pArgs, i, Py_False);
            } catch(std::exception &e) {
                return false;
            }
            return true;
        }
        bool pythonUdf::set_arg_at(int i, double const& arg) {
            if((i >= arg_count) | (arg_types[i] != PyUdfSchema::PyUdfArgType::DOUBLE)) {
                return false;
            }
            try {
                PyTuple_SetItem(pArgs, i, PyFloat_FromDouble(arg));
            } catch(std::exception &e) {
                return false;
            }
            return true;
        }
        bool pythonUdf::set_arg_at(int i, std::string const& arg) {
            if((i >= arg_count) | (arg_types[i] != PyUdfSchema::PyUdfArgType::STRING)) {
                return false;
            }
            try {
                PyTuple_SetItem(pArgs, i, PyUnicode_FromStringAndSize(arg.c_str(), arg.length()));
            } catch(std::exception &e) {
                return false;
            }
            return true;
        }
        bool pythonUdf::set_arg_at(int i, PyObject* arg) {
            try {
                PyTuple_SetItem(pArgs, i, arg);
            } catch(std::exception &e) {
                return false;
            }
            return true;
        }
        
        //重置numpy Arrays
        bool pythonUdf::resetNumpyArrays(int newBatchSize) {
            for (int i = 0; i < arg_count; i++) {
                PyArray_XDECREF((PyArrayObject *)numpyArrays[i]);
            }
            npy_intp numpySize[1] = {newBatchSize};
            for (int i = 0; i < arg_count; i++) {
                switch(arg_types[i]) {
                    case PyUdfSchema::PyUdfArgType::INTEGER: { 
                        numpyArrays[i] = PyArray_EMPTY(1, numpySize, NPY_INT32, 0);
                        break;
                    }
                    case PyUdfSchema::PyUdfArgType::DOUBLE: {
                        numpyArrays[i] = PyArray_EMPTY(1, numpySize, NPY_FLOAT64, 0);
                        break;
                    }
                    case PyUdfSchema::PyUdfArgType::STRING: {
                        numpyArrays[i] = PyArray_New(&PyArray_Type, 1, numpySize, NPY_OBJECT, NULL, NULL, 0, 0, NULL);
                        break;
                    }
                    default: {
                        return false;
                        break;
                    }
                }
            }
            currentIndex = 0;
            batch_size = newBatchSize;
            return true;
        }
        
        //探测剩余容量
        int pythonUdf::detectCapacity(int size) {
            if(size <= 0)
                return -1; // 发生错误
            // 返回可用容量，此时必有空余容量
            return (currentIndex + size) <= batch_size ? size : (batch_size - currentIndex);
        }
        //移动下标  
        bool pythonUdf::moveIndex(int size) {
            if(size <= 0 | currentIndex + size > batch_size)
                return false;
            if(currentIndex + size == batch_size) { //进行执行过程，会重置下标与容量
                executeNumpyArrays();
                returnObResult();
            } else { //移动下标
                currentIndex += size;
            }
            return true;
        }

        //插入numpyArrays,危险
        bool pythonUdf::insertNumpyArray(int i, int j, char* ptr, long length) {
            return PyArray_SETITEM((PyArrayObject *)numpyArrays[i], (char *)PyArray_GETPTR1((PyArrayObject *)numpyArrays[i], currentIndex + j), 
                PyUnicode_FromStringAndSize(ptr, length));
        }
        bool pythonUdf::insertNumpyArray(int i, int j, long const& arg) {
            return PyArray_SETITEM((PyArrayObject *)numpyArrays[i], (char *)PyArray_GETPTR1((PyArrayObject *)numpyArrays[i], currentIndex + j), PyLong_FromLong(arg));
        }
        bool pythonUdf::insertNumpyArray(int i, int j, double const& arg) {
            return PyArray_SETITEM((PyArrayObject *)numpyArrays[i], (char *)PyArray_GETPTR1((PyArrayObject *)numpyArrays[i], currentIndex + j), PyFloat_FromDouble(arg));
        }
        
        
        //执行初始化代码
        bool pythonUdf::execute_initial() {
            
            //execute pInitial
            try {
                PyObject_CallObject(pInitial, NULL);
            } catch(std::exception &e) {
                return false;
            }
            return true;
        }
        //执行
        bool pythonUdf::execute() {
            //reset
            //pResult = NULL;
            //execute pFun
            try {
                pResult = PyObject_CallObject(pFunc, pArgs);
                if(!pResult) {
                    process_python_exception();
                    return false;
                }
                return true;
            } catch(...) {
                process_python_exception();
                return false;
            }
        }
        //装载并执行numpy array
        bool pythonUdf::executeNumpyArrays() {
            try {
                struct timeval t1, t2, tsub;
                gettimeofday(&t1, NULL);

                if (currentIndex < batch_size) {
                    PyArray_Dims shape;
                    npy_intp size[1] = {currentIndex};
                    shape.ptr = size;
                    shape.len = 1;
                    for (int i = 0; i < arg_count; i++) {
                        PyArray_Resize((PyArrayObject *)numpyArrays[i], &shape, 0, NPY_ANYORDER);
                    }
                }
                for (int i = 0; i < arg_count; i++) {
                    PyTuple_SetItem(pArgs, i, numpyArrays[i]);
                }
                bool ret = execute();

                gettimeofday(&t2, NULL);
                std::ofstream out;
                out.open(fileName, std::ios::app);
                if(out.is_open()){
                    double tu;
                    timersub(&t2, &t1, &tsub);
                    tu = tsub.tv_sec*1000 + (1.0 * tsub.tv_usec)/1000;
                    //out << "real batch size: " << real_param  << std::endl;
                    out << "execute Numpy Arrays: " << tu << " ms" << std::endl;
                    out.close();
                }

                return ret;
            } catch(std::exception &e) {
                return false;
            }
        }

        //获取执行结果->重载
        bool pythonUdf::get_result(long& result) {
            if((pResult == NULL) | (*rt_type != PyUdfSchema::PyUdfRetType::LONG)){
                return false;
            }
            try {
                result = PyLong_AsLong(pResult);
            } catch(std::exception &e) {
                return false;
            }
            return true;
        }
        bool pythonUdf::get_result(bool& result) {
            if((pResult == NULL) | (*rt_type != PyUdfSchema::PyUdfRetType::BOOLEAN)){
                return false;
            }
            try {
                //

            } catch(std::exception &e) {
                return false;
            }
            return true;
        }
        bool pythonUdf::get_result(double& result) {
            if((pResult == NULL) | (*rt_type != PyUdfSchema::PyUdfRetType::DOUBLE)){
                return false;
            }
            try {
                result = PyFloat_AsDouble(pResult);
            } catch(std::exception &e) {
                return false;
            }
            return true;
        }
        //bytes有不同实现
        bool pythonUdf::get_result(std::string& result) {
            if((pResult == NULL) | (*rt_type != PyUdfSchema::PyUdfRetType::BYTES)){
                return false;
            }
            try {
                result = PyBytes_AsString(pResult);
                //result = PyByteArray_AsString(pResult);
                //result = PyUnicode_AsUTF8(pResult);
            } catch(std::exception &e) {
                return false;
            }
            return true;
        }
        //获取numpy array
        bool pythonUdf::get_result(PyObject*& result) {
            try {
                result = pResult;
            } catch(std::exception &e) {
                return false;
            }
            return true;
        }

        //test ptr
        void pythonUdf::setptr() {
            //test
            if(resultptr != nullptr) {
                resultptr[0].set_int(1000);
                resultptr = nullptr;
            }
        }
        
        //异常输出
        void pythonUdf::message_error_dialog_show(char* buf) {
            std::ofstream ofile;
            if(ofile)
            {
                ofile.open("/home/test/log/expedia/log", std::ios::out);

                ofile << buf;

                ofile.close();
            }
            return;
        }

        //异常处理
        void pythonUdf::process_python_exception() {
            char buf[65536], *buf_p = buf;
            PyObject *type_obj, *value_obj, *traceback_obj;
            PyErr_Fetch(&type_obj, &value_obj, &traceback_obj);
            if (value_obj == NULL)
                return;

            PyObject *pstr = PyObject_Str(value_obj);

            const char* value = PyUnicode_AsUTF8(pstr);

            size_t szbuf = sizeof(buf);
            int l;
            PyCodeObject *codeobj;

            l = snprintf(buf_p, szbuf, ("Error Message:\n%s"), value);
            buf_p += l;
            szbuf -= l;

            if (traceback_obj != NULL) {
                l = snprintf(buf_p, szbuf, ("\n\nTraceback:\n"));
                buf_p += l;
                szbuf -= l;

                PyTracebackObject *traceback = (PyTracebackObject *)traceback_obj;
                for (; traceback && szbuf > 0; traceback = traceback->tb_next) {
                    //codeobj = traceback->tb_frame->f_code;
                    codeobj = PyFrame_GetCode(traceback->tb_frame);
                    l = snprintf(buf_p, szbuf, "%s: %s(# %d)\n",
                        PyUnicode_AsUTF8(PyObject_Str(codeobj->co_name)),
                        PyUnicode_AsUTF8(PyObject_Str(codeobj->co_filename)),
                        traceback->tb_lineno);
                    buf_p += l;
                    szbuf -= l;
                }
            }

            message_error_dialog_show(buf);

            Py_XDECREF(type_obj);
            Py_XDECREF(value_obj);
            Py_XDECREF(traceback_obj);
            
        }

        //initial
        bool pythonUdf::addNewObResult(ObDatum* resultHead, int length) {
            currentResult->next = new ObResult();
            bool ret = currentResult->next->init(resultHead, length);
            if(ret)
                currentResult = currentResult->next;
            return ret;
        }
        //insert
        bool pythonUdf::insertObResult(int loc, int pos) {
            return currentResult->setIndex(loc, pos);
        }
        //getIndex
        int pythonUdf::getIndexObResult(int loc) {
            return currentResult->getIndex(loc);
        }
        //return
        bool pythonUdf::returnObResult() {
            PyArrayObject *resultArray = (PyArrayObject *)pResult;
            currentResult = ObResultArray;
            int j = 0;
            switch(*(rt_type)) {
                case PyUdfSchema::PyUdfRetType::PYUNICODE: { 
                    while(currentResult != nullptr) {
                        for (int i = 0; i < currentResult->size; i++) {
                            //类型转换
                            PyObject *value = PyArray_GETITEM(resultArray, (char *)PyArray_GETPTR1(resultArray, j++));
                            currentResult->ptrHead[currentResult->index[i]].set_string(common::ObString(PyUnicode_AS_DATA(value)));
                        }
                        currentResult = currentResult->next;
                    }
                    break;
                }
                case PyUdfSchema::PyUdfRetType::DOUBLE: {
                    while(currentResult != nullptr) {
                        for (int i = 0; i < currentResult->size; i++) {
                            //类型转换
                            PyObject *value = PyArray_GETITEM(resultArray, (char *)PyArray_GETPTR1(resultArray, j++));
                            currentResult->ptrHead[currentResult->index[i]].set_double(PyFloat_AsDouble(value));
                        }
                        currentResult = currentResult->next;
                    }
                    break;
                }
                case PyUdfSchema::PyUdfRetType::LONG: {
                    while(currentResult != nullptr) {
                        for (int i = 0; i < currentResult->size; i++) {
                            //类型转换
                            PyObject *value = PyArray_GETITEM(resultArray, (char *)PyArray_GETPTR1(resultArray, j++));
                            currentResult->ptrHead[currentResult->index[i]].set_int(PyLong_AsLong(value));
                        }
                        currentResult = currentResult->next;
                    }
                    break;
                }
                default: {
                    return false;
                    break;
                }
            }
            //reset
            //暂时batch_size不变
            return (resetObResult() && resetNumpyArrays(batch_size));
        }
        //reset
        bool pythonUdf::resetObResult() {
            delete ObResultArray;
            ObResultArray = new ObResult();
            currentResult = ObResultArray;
            return true;
        }
        //end -> execute -> return
        bool pythonUdf::endOperatorSignal() {
            if(currentIndex != 0) {
                _import_array(); //load numpy api
                if(!executeNumpyArrays())
                    return false;
                return returnObResult();
            }
            return false;
        }

        //------------------------------ OB result-------------------------------- 
        ObResult::ObResult() {
            ptrHead = nullptr;
            index = nullptr;
            size = 0;
            next = nullptr;
        }

        ObResult::~ObResult() {
            //只删除开辟的指针空间，不对数据库内数据进行操作
            ptrHead = nullptr;
            delete[] index;
            index = nullptr;
            delete next;
            next = nullptr;
        }

        bool ObResult::init(ObDatum* resultHead, int length) {
            if(resultHead == NULL | length < 0)
                return false;
            size = length;
            ptrHead = resultHead;
            index = new int[size];
            return true;
        }

        
        //类型判断应该在udf层完成 ***
        /*bool ObResult::setType(ObObjType obType) {
            //根据ObObjtype判断是什么类型
            switch(obType) {
                case ObCharType:
                case ObVarcharType:
                case ObTinyTextType:
                case ObTextType:
                case ObMediumTextType:
                case ObLongTextType: {
                    type = PyUdfSchema::PyUdfRetType::PYUNICODE;
                    break;
                }
                case ObTinyIntType:
                case ObSmallIntType:
                case ObMediumIntType:
                case ObInt32Type:
                case ObIntType: {
                    type = PyUdfSchema::PyUdfRetType::LONG;
                    break;
                }
                case ObDoubleType: {
                    type = PyUdfSchema::PyUdfRetType::DOUBLE;
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
        }*/

        bool ObResult::setIndex(int loc, int pos) {
            if(loc < 0 || loc > size)
                return false;
            index[loc] = pos;
            return true;
        }

        int ObResult::getIndex(int loc) {
            if(loc < 0)
                return false;
            return index[loc];
        }

        bool ObResult::setNext(ObResult* Next) {
            if(Next == nullptr)
                return false;
            next = Next;
            return true;
        }
    }
}