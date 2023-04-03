#ifndef _PYTHON_UDF_SCHEMA_H_
#define _PYTHON_UDF_SCHEMA_H_

#endif

namespace oceanbase 
{
    namespace sql 
    {
        class PyUdfSchema {
            public:
                //枚举参数类型
                enum class PyUdfArgType {
                    UNINITIAL, //illegal
                    INTEGER, //Long
                    BOOLEAN,
                    DOUBLE,
                    STRING, //Bytes
                    PyObj //PyObject --> numpy array
                }; 
                //枚举计算结果后返回值类型
                enum class PyUdfRetType {
                    UNINITIAL, //illegal
                    LONG,
                    BOOLEAN,
                    DOUBLE,
                    BYTES, 
                    BYTES_ARRAY,
                    PYUNICODE,
                    PyObj //PyObject --> numpy array
                };
        };
    }

}