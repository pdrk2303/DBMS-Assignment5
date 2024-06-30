package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.util.List;

/*
 * PProjectFilter is a relational operator that represents a Project followed by a Filter.
 * You need to write the entire code in this file.
 * To implement PProjectFilter, you can extend either Project or Filter class.
 * Define the constructor accordinly and override the methods as required.
 */
public class PProjectFilter extends Project implements PRel {
    private final RexNode condition;
    public PProjectFilter (
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType, RexNode condition) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        this.condition = condition;
    }

    @Override
    public Project copy(RelTraitSet relTraitSet, RelNode relNode, List<RexNode> list, RelDataType relDataType) {
        return new PProjectFilter(getCluster(), relTraitSet, relNode, list, relDataType, condition);
    }

    public String toString() {
        return "PProjectFilter";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProjectFilter");
        /* Write your code here */
//        System.out.println("PProjectFilter Open");
        RelNode child = getInput();
        PRel pRel = (PRel) child;
        return pRel.open();
//        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProjectFilter");
        /* Write your code here */
        return;
    }

    private Object handleInputRef(RexInputRef op, Object[] row) {
        int id = op.getIndex();
        if (id >= 0 && id < row.length) {
            return row[id];
        } else {
            throw new IllegalArgumentException("Invalid column index: " + id);
        }
    }

    private Object handleArithmetic(RexCall op, Object[] row) {
        SqlOperator operator = op.getOperator();
        List<RexNode> operands = op.getOperands();
        Object a = resultant_operand(operands.get(0), row);
        Object b = resultant_operand(operands.get(1), row);

        if (a == null || b == null) {
            return null;
        }

        BigDecimal left = convertToBigDecimal(a);
        BigDecimal right = convertToBigDecimal(b);

        if (left == null || right == null) {
            return null;
        }

        switch (operator.getKind()) {
            case PLUS:
                return left.add(right);
            case MINUS:
                return left.subtract(right);
            case TIMES:
                return left.multiply(right);
            case DIVIDE:
                return left.divide(right);
            default:
                return null;
        }
    }

    private BigDecimal convertToBigDecimal(Object value) {
        if (value instanceof Integer) {
            return BigDecimal.valueOf((Integer) value);
        } else if (value instanceof Float) {
            return BigDecimal.valueOf((Float) value);
        } else if (value instanceof Double) {
            return BigDecimal.valueOf((Double) value);
        } else if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        return null;
    }

    private Object resultant_operand(RexNode op, Object[] row) {
        if (op instanceof RexInputRef) {
            return handleInputRef((RexInputRef) op, row);
        } else if (op instanceof RexLiteral) {
            return ((RexLiteral) op).getValue();
        } else {
            return handleArithmetic((RexCall) op, row);
        }
    }

    private boolean result(SqlOperator op, List<RexNode> ops, Object[] row) {
        SqlKind op_kind = op.getKind();

        if (op_kind == SqlKind.NOT) {
            if (ops.size() == 1) {
                RexCall cond = (RexCall) ops.get(0);
                return !(result(cond.getOperator(), cond.getOperands(), row));
            }
        } else if (op_kind == SqlKind.AND) {
            boolean f = true;
            for (int i=0; i<ops.size(); i++) {
                RexCall cond = (RexCall) ops.get(i);
                if (! result(cond.getOperator(), cond.getOperands(), row)) {
                    f = false;
                    break;
                }
            }
            if (f) {
                return true;
            } else {
                return false;
            }
        } else if (op_kind == SqlKind.OR) {
            boolean f = false;
            for (int i=0; i<ops.size(); i++) {
                RexCall cond = (RexCall) ops.get(i);
                if (result(cond.getOperator(), cond.getOperands(), row)) {
                    f = true;
                    break;
                }
            }
            if (f) {
                return true;
            } else {
                return false;
            }
        } else if (op_kind == SqlKind.EQUALS || op_kind == SqlKind.GREATER_THAN || op_kind == SqlKind.GREATER_THAN_OR_EQUAL || op_kind == SqlKind.LESS_THAN || op_kind == SqlKind.LESS_THAN_OR_EQUAL) {
            Object a = resultant_operand(ops.get(0), row);
            Object b = resultant_operand(ops.get(1), row);
            if (a == null || b == null) {
                return false;
            }

            return compareValues(op_kind, a, b);
        }

        return false;
    }

    private boolean compareValues(SqlKind opKind, Object a, Object b) {
        if (a instanceof Integer || a instanceof Float || a instanceof Double || a instanceof BigDecimal) {
            BigDecimal left = convertToBigDecimal(a);
            BigDecimal right = convertToBigDecimal(b);
            if (left == null || right == null) {
                return false;
            }
            int res = left.compareTo(right);
            return handleComparison(opKind, res);
        } else if (a instanceof String) {
            String left = (String) a;
            NlsString nls = (NlsString) b;
            String right = nls.getValue();
            int res = ((Comparable) left).compareTo(right);
            return handleComparison(opKind, res);
        } else if (a instanceof Boolean) {
            Boolean left = (Boolean) a;
            int res = Boolean.compare(left, (Boolean) b);
            return handleComparison(opKind, res);
        }

        return false;
    }

    private boolean handleComparison(SqlKind opKind, int result) {
        switch (opKind) {
            case EQUALS:
                return result == 0;
            case GREATER_THAN:
                return result > 0;
            case GREATER_THAN_OR_EQUAL:
                return result >= 0;
            case LESS_THAN:
                return result < 0;
            case LESS_THAN_OR_EQUAL:
                return result <= 0;
            default:
                return false;
        }
    }

    private Object[] current_row;
    private int has_current_row = 0;
    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProjectFilter has next");
        /* Write your code here */
//        System.out.println("PProjectFilter hasNext");
        RelNode child = getInput();
        PRel pRel = (PRel) child;
//        System.out.println("PFilter hasnext: " + pRel);
        boolean flag = pRel.hasNext();
        RexCall call = (RexCall) condition;
        SqlOperator operator = call.getOperator();
        List<RexNode> operands = call.getOperands();

        while (flag && has_current_row == 0) {
            Object[] row = pRel.next();
            while(row == null) {
                if (pRel.hasNext()) {
                    row = pRel.next();
                } else {
                    return false;
                }
            }

            boolean res = result(operator, operands, row);
            if (res) {
                has_current_row = 1;
                current_row = row;
                return true;
            }
            flag = pRel.hasNext();
        }

        return false;
//        return true;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProjectFilter");
        /* Write your code here */
//        System.out.println("PProjectFilter Next");
//        RelNode child = getInput();
//        PRel pRel = (PRel) child;
//        if (!pRel.hasNext()) {
//            return null;
//        }
//        Object[] row = pRel.next();

        Object[] row;
        List<? extends RexNode> projects = getProjects();
        Object[] result = new Object[projects.size()];

        if (has_current_row == 1) {
            has_current_row = 0;
            row = current_row;
            for (int i=0; i<projects.size(); i++) {
                RexNode r = projects.get(i);
                Object row_value = resultant_operand(r, row);
                if (row_value != null) {
                    result[i] = resultant_operand(r, row);
                } else {
                    return null;
                }
            }
            return result;
        }

        RelNode child = getInput();
        PRel pRel = (PRel) child;
        while((row = pRel.next()) != null) {
            for (int i=0; i<projects.size(); i++) {
                RexNode r = projects.get(i);
                Object row_value = resultant_operand(r, row);
                if (row_value != null) {
                    result[i] = resultant_operand(r, row);
                } else {
                    return null;
                }
            }
            return result;

        }

        return null;

    }
}
