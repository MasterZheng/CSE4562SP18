package edu.buffalo.www.cse4562.Evaluate;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.List;

public class expProcess {

    private List<Expression> expressions;

    public expProcess(Expression expression) {
        this.expressions = new ArrayList<>();
        if (expression instanceof AndExpression) {
                Expression left = ((AndExpression) expression).getLeftExpression();
                Expression right = ((AndExpression) expression).getRightExpression();
                //the left and right subExpression of OrExpression should be with the same table.Should not be divided
                if (!(right instanceof AndExpression)) {
                    expressions.add(right);
                }else {
                    expressions.addAll(parseAndExpression(right));
                }
                if (!(left instanceof AndExpression)) {
                    expressions.add(left);
                } else {
                    expressions.addAll(parseAndExpression(left));
                }
        } else {
            expressions.add(expression);
        }
    }

    public List<Expression> parseAndExpression(Expression expression){
        List<Expression> list = new ArrayList<>();
        if (expression instanceof AndExpression){
            Expression left = ((AndExpression) expression).getLeftExpression();
            Expression right = ((AndExpression) expression).getRightExpression();
            if (left instanceof AndExpression){
                list.addAll(parseAndExpression(left));
            }else {
                list.add(left);
            }
            if (right instanceof AndExpression){
                list.addAll(parseAndExpression(right));
            }else {
                list.add(right);
            }
        }
        return list;
    }
//    public boolean isRelated(Table t, Expression e) {
//        boolean flag = false;
//        //todo finish the condition
//        if (e instanceof EqualsTo) {
//            flag = judge(t,((EqualsTo) e).getLeftExpression(),((EqualsTo) e).getRightExpression());
//        }else if (e instanceof NotEqualsTo){
//            flag = judge(t,((NotEqualsTo) e).getLeftExpression(),((NotEqualsTo) e).getRightExpression());
//        }else if (e instanceof GreaterThan){
//            flag = judge(t,((GreaterThan) e).getLeftExpression(),((GreaterThan) e).getRightExpression());
//        }else if (e instanceof  GreaterThanEquals){
//            flag = judge(t,((GreaterThanEquals) e).getLeftExpression(),((GreaterThanEquals) e).getRightExpression());
//        }else if (e instanceof MinorThan){
//            flag = judge(t,((MinorThan) e).getLeftExpression(),((MinorThan) e).getRightExpression());
//        }else if (e instanceof MinorThanEquals){
//            flag = judge(t,((MinorThanEquals) e).getLeftExpression(),((MinorThanEquals) e).getRightExpression());
//        }
//        return flag;
//    }
//
//    private boolean judge(Table t,Expression le,Expression re){
//        Boolean flag = false;
//        String alisa = t.getAlias();
//        if (le instanceof Column) {
//            Table left = ((Column)le).getTable();
//            if (left != null && left.getName().equals(t.getName())
//                    ||(alisa!= null && left.getName().equals(alisa))) {
//                flag = true;
//            }
//        }
//        if (re instanceof Column) {
//            Table right = ((Column)re).getTable();
//            if (right != null && right.getName().equals(t.getName())
//                    ||(alisa!= null && right.getName().equals(alisa))) {
//                flag = true;
//            }
//        }
//        return flag;
//    }

    public int isRelated(Table t, Expression e) {
        int flag = 0;
        //todo finish the condition
        if (e instanceof EqualsTo) {
            flag = judge(t, ((EqualsTo) e).getLeftExpression(), ((EqualsTo) e).getRightExpression());
        } else if (e instanceof NotEqualsTo) {
            flag = judge(t, ((NotEqualsTo) e).getLeftExpression(), ((NotEqualsTo) e).getRightExpression());
        } else if (e instanceof GreaterThan) {
            flag = judge(t, ((GreaterThan) e).getLeftExpression(), ((GreaterThan) e).getRightExpression());
        } else if (e instanceof GreaterThanEquals) {
            flag = judge(t, ((GreaterThanEquals) e).getLeftExpression(), ((GreaterThanEquals) e).getRightExpression());
        } else if (e instanceof MinorThan) {
            flag = judge(t, ((MinorThan) e).getLeftExpression(), ((MinorThan) e).getRightExpression());
        } else if (e instanceof MinorThanEquals) {
            flag = judge(t, ((MinorThanEquals) e).getLeftExpression(), ((MinorThanEquals) e).getRightExpression());
        }
        return flag;
    }

    private int judge(Table t, Expression le, Expression re) {
        int flag = 0;
        String alisa = t.getAlias();
        if (le instanceof Column && re instanceof Column) {
            Table left = ((Column) le).getTable();
            Table right = ((Column) re).getTable();
            if (left != null && left.getName().equals(t.getName())
                    || (alisa != null && left.getName().equals(alisa))) {
                flag = 1;
            } else if (right != null && right.getName().equals(t.getName())
                    || (alisa != null && right.getName().equals(alisa))) {
                flag = 1;
            }
        } else if (le instanceof Column) {
            Table left = ((Column) le).getTable();
            if (left != null && left.getName().equals(t.getName())
                    || (alisa != null && left.getName().equals(alisa))) {
                flag = 2;
            }
        } else if (re instanceof Column) {
            Table right = ((Column) re).getTable();
            if (right != null && right.getName().equals(t.getName())
                    || (alisa != null && right.getName().equals(alisa))) {
                flag = 2;
            }
        }
        return flag;
    }

    public Expression mergeAndExpression(Expression e1, Expression e2) {
        if (e1 != null) {
            return new AndExpression(e1, e2);
        } else {
            return e2;
        }
    }

    public List<Expression> getExpressions() {
        return expressions;
    }


}
