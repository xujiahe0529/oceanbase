#ifndef _OB_EXPR_PYTHON_UDF_
#define  _OB_EXPR_PYTHON_UDF_

#include  "sql/engine/expr/ob_expr_operator.h"

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
                           const common::ObObj *objs_array,
                           int64_t param_num,
                           common::ObExprCtx &expr_ctx) const;
  
  virtual int cg_expr(ObExprCGCtx&  op_cg_ctx,
                      const  ObRawExpr&  raw_expr,
                      ObExpr&  rt_expr) const  override;

  static int eval_python_udf(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             ObDatum &res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPythonUdf);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif