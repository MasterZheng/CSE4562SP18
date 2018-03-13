package edu.buffalo.www.cse4562.Evaluate;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.List;

public class expProcess {

    private List<Expression> expressions;

    public expProcess(Expression expression) {
        this.expressions = new ArrayList<>();
        while (expression instanceof AndExpression) {
            Expression left = ((AndExpression) expression).getLeftExpression();
            Expression right = ((AndExpression) expression).getRightExpression();
            //the left and right subExpression of OrExpression should be with the same table.Should not be divided
            if (!(right instanceof AndExpression)) {
                expressions.add(right);
            }
            if (!(left instanceof AndExpression)) {
                expressions.add(left);
                break;
            } else {
                expression = left;
            }
        }
    }

    public boolean isRelated(Table t, Expression e) {
        boolean flag = false;
        if (e instanceof EqualsTo) {
            if (((EqualsTo) e).getLeftExpression() instanceof Column) {
                Table left = ((Column) ((EqualsTo) e).getLeftExpression()).getTable();
                if (left != null && left.getName().equals(t.getName())) {
                    flag = true;
                }
            }
            if (((EqualsTo) e).getRightExpression() instanceof Column) {
                Table right = ((Column) ((EqualsTo) e).getRightExpression()).getTable();
                if (right != null && right.getName().equals(t.getName())) {
                    flag = true;
                }
            }
        }
        return flag;
    }

    public Expression mergeAndExpression(Expression e1, Expression e2) {
        if (e1!=null){
            return new AndExpression(e1, e2);
        }else {
            return e2;
        }
    }

    public List<Expression> getExpressions() {
        return expressions;
    }


}
