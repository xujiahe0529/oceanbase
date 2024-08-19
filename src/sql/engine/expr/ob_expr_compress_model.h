#ifndef _OB_EXPR_COMPRESS_MODEL_
#define  _OB_EXPR_COMPRESS_MODEL_
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include  "sql/engine/expr/ob_expr_operator.h"

namespace  oceanbase {
namespace  sql {
class  ObExprCompressModel : public  ObExprOperator {
public:
  explicit  ObExprCompressModel(common::ObIAllocator&  alloc);
  virtual  ~ObExprCompressModel();

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

  static int eval_compress_model(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             ObDatum &res);

};

} /* namespace sql */
} /* namespace oceanbase */

#endif