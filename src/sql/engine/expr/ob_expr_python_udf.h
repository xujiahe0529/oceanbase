#ifndef _OB_EXPR_PYTHON_UDF_
#define  _OB_EXPR_PYTHON_UDF_

#include  "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/python_udf_engine/python_udf_engine.h"

namespace  oceanbase {
namespace  sql {
class  ObExprPythonUdf : public  ObExprOperator {
public:
  explicit  ObExprPythonUdf(common::ObIAllocator&  alloc);
  virtual  ~ObExprPythonUdf();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int calc_resultN(common::ObObj &result,
                           const common::ObObj *objs,
                           int64_t param_num,
                           common::ObExprCtx &expr_ctx) const;
  
  virtual int cg_expr(ObExprCGCtx&  op_cg_ctx,
                      const  ObRawExpr&  raw_expr,
                      ObExpr&  rt_expr) const  override;

  static int eval_python_udf(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             ObDatum &res);

  static int eval_python_udf_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                   const ObBitVector &skip, const int64_t batch_size);

  static int eval_python_udf_batch_buffer(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const int64_t batch_size);

  static int get_python_udf(pythonUdf* &pyudf, const ObExpr& expr);

  static int obdatum2array(const ObDatum *argdatum, const ObObjType &type, PyObject *&array, const int64_t batch_size);
  static int obdatum2array(const ObDatumVector &vector, const ObObjType &type, PyObject *&array, 
                           const int64_t batch_size, const int64_t real_param, 
                           const ObBitVector &skip, ObBitVector &eval_flags);

  static int numpy2value(PyObject *numpyarray,const int loc, PyObject *&value);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPythonUdf);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif