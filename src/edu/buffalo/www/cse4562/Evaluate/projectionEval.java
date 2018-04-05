package edu.buffalo.www.cse4562.Evaluate;


import edu.buffalo.www.cse4562.RA.*;
import edu.buffalo.www.cse4562.Table.TableObject;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

public class projectionEval {
    private List<SelectItem> selectItemList;

    public projectionEval(List<SelectItem> selectItemList) {
        this.selectItemList = selectItemList;
    }

    public List<Column> parseProjection(List<TableObject> involvedTables) {
        List<Column> result = new ArrayList<>();
        for (SelectItem s : selectItemList) {
            if (s instanceof AllColumns) {
                for (TableObject t : involvedTables) {
                    result.addAll(t.getColumnInfo());
                }
                break;
            } else if (s instanceof AllTableColumns) {
                for (TableObject t : involvedTables) {
                    if (t.getAlisa().equals(((AllTableColumns) s).getTable().getName()) ||
                            t.getAlisa().equals(((AllTableColumns) s).getTable().getAlias()) ||
                            t.getTableName().equals(((AllTableColumns) s).getTable().getName()) ||
                            t.getTableName().equals(((AllTableColumns) s).getTable().getAlias())) {
                        result.addAll(t.getColumnInfo());
                    }
                }
            } else {
                Expression e = ((SelectExpressionItem) s).getExpression();
                if (e instanceof Column) {
                    result.add((Column) e);
                } else if (e instanceof Function) {
                    List<Expression> parameters = ((Function) e).getParameters().getExpressions();
                    for (Expression exp : parameters) {
                        if (exp instanceof Column) {
                            result.add((Column) exp);
                        }
                    }
                } else if (e instanceof Addition) {
                    Expression left = ((Addition) e).getLeftExpression();
                    Expression right = ((Addition) e).getRightExpression();
                    if (left instanceof Column)
                        result.add((Column) left);
                    if (right instanceof Column)
                        result.add((Column) right);
                } else if (e instanceof Subtraction) {
                    Expression left = ((Subtraction) e).getLeftExpression();
                    Expression right = ((Subtraction) e).getRightExpression();
                    if (left instanceof Column)
                        result.add((Column) left);
                    if (right instanceof Column)
                        result.add((Column) right);
                } else if (e instanceof Multiplication) {
                    Expression left = ((Multiplication) e).getLeftExpression();
                    Expression right = ((Multiplication) e).getRightExpression();
                    if (left instanceof Column)
                        result.add((Column) left);
                    if (right instanceof Column)
                        result.add((Column) right);
                } else if (e instanceof Division) {
                    Expression left = ((Division) e).getLeftExpression();
                    Expression right = ((Division) e).getRightExpression();
                    if (left instanceof Column)
                        result.add((Column) left);
                    if (right instanceof Column)
                        result.add((Column) right);
                }
            }
        }
        return result;
    }

    public List<Column> parseOrderBy(List<OrderByElement> orderBy) {
        List<Column> result = new ArrayList<>();
        for (OrderByElement o : orderBy) {
            if (o.getExpression() instanceof Column) {
                result.add((Column) o.getExpression());
            }
        }
        return result;
    }

    public List<Column> usefulCol(Expression where, List<OrderByElement> orderBy, List<Column> groupByColumnReference, List<TableObject> involvedTables) {
        //find the useful columns in selectItems and where
        List<Column> result = new ArrayList<>();
        //process projection
        result.addAll(parseProjection(involvedTables));

        //process selection
        if (where != null) {
            selectionEval selectionEval = new selectionEval(where);
            List<Expression> whereList = new ArrayList<>();
            whereList = selectionEval.parse2List(whereList, where);
            result.addAll(selectionEval.parseSelect(whereList));
        }

        //process orderby
        if (orderBy != null) {
            result.addAll(parseOrderBy(orderBy));
        }

        //process groupBy
        if (groupByColumnReference != null) {
            result.addAll(groupByColumnReference);
        }
        //eliminate duplicate
        //convert List to set to remove the duplicate elements
        Set<Column> resultSet = new HashSet<>(result);
        result = new ArrayList<>(resultSet);

        return result;
    }

    public void pushdownProject(RANode join, List<Column> selectItemList, List<TableObject> involvedTables) {
        for (int i = 0; i < selectItemList.size(); i++) {
            Column c = selectItemList.get(i);
            Table table = c.getTable();
            if (!table.toString().equals("null")) {
                String tableName = c.getTable().getName().toUpperCase();
                if (join.getRightNode() instanceof RATable) {
                    RATable rightTable = (RATable) join.getRightNode();
                    String rightName = rightTable.getTable().getName().toUpperCase();
                    String rightAlisa = rightTable.getTable().getAlias().toUpperCase();
                    if (tableName.equals(rightName) || tableName.equals(rightAlisa)) {
                        rightTable.addItemIntoColInf(new Column(table, c.getColumnName()));
                        continue;
                    }
                }
                if (join.getLeftNode() instanceof RATable) {
                    RATable leftTable = (RATable) join.getLeftNode();
                    String leftName = leftTable.getTable().getName().toUpperCase();
                    String leftAlisa = leftTable.getTable().getAlias();
                    leftAlisa = leftAlisa == null ? null : leftAlisa.toUpperCase();
                    if (tableName.equals(leftName) || tableName.equals(leftAlisa)) {
                        leftTable.addItemIntoColInf(new Column(table, c.getColumnName()));
                        continue;
                    }
                }

                if (join.getLeftNode() instanceof RAJoin) {
                    RAJoin leftTable = (RAJoin) join.getLeftNode();
                    pushdownProject(leftTable, selectItemList.subList(i, i + 1), involvedTables);
                }
            } else {
                //只涉及1个表查询时，将无表名的列直接归入当前表
                if (involvedTables.size() == 1) {
                    RANode left = join.getLeftNode();
                    if (left instanceof RATable &&
                            ((RATable) left).getTable().getName().equals(involvedTables.get(0).getTableName())) {
                        ((RATable) left).addItemIntoColInf(c);
                    }
                } else {
                    //todo
                    //涉及多表时，查询列名在各表中是否存在，存在则归入，如都不存在，则存在子查询，将列归入子查询

                    RANode left = join.getLeftNode();
                    RANode right = join.getRightNode();
                    if (right instanceof RATable) {
                        for (TableObject t : involvedTables) {
                            if (((RATable) right).getTable().getName().equals(t.getTableName()) ||
                                    ((RATable) right).getTable().getName().equals(t.getAlisa())) {
                                c.setTable(((RATable) right).getTable());
                                if (t.getColumnInfo().contains(c)) {
                                    ((RATable) right).addItemIntoColInf(c);
                                    break;
                                }
                            }
                        }
                    }
                    if (left instanceof RATable) {
                        for (TableObject t : involvedTables) {
                            if (((RATable) left).getTable().getName().equals(t.getTableName()) ||
                                    ((RATable) left).getTable().getName().equals(t.getAlisa())) {
                                c.setTable(((RATable) left).getTable());
                                if (t.getColumnInfo().contains(c)) {
                                    ((RATable) left).addItemIntoColInf(c);
                                    break;
                                }
                            }
                        }
                    } else if (left instanceof RAJoin) {
                        pushdownProject(left, selectItemList.subList(i, i + 1), involvedTables);
                    }


                }

            }

        }
    }
}
