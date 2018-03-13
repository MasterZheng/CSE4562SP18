package edu.buffalo.www.cse4562.Evaluate;

import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class evaluate extends Eval {
    static Logger logger = Logger.getLogger(evaluate.class.getName());
    private Tuple tupleLeft;
    private Tuple tupleRight;
    private Expression expression;
    private List<Object> selectList;
    private ArrayList<TableObject> tableList;

    public evaluate(Tuple tupleLeft, Tuple tupleRight, Expression expression) {
        //selection
        this.tupleLeft = tupleLeft;
        this.tupleRight = tupleRight;
        this.expression = expression;
    }

    public evaluate(Tuple tupleLeft, List<Object> list, ArrayList<TableObject> tableList) {
        //projection
        this.tupleLeft = tupleLeft;
        this.selectList = list;
        this.tableList = tableList;

    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException {
        String colTable = column.getTable().getName();
        if (colTable != null) {
            if (tupleLeft.getTableName().contains(colTable) || tupleRight == null) {
                return tupleLeft.getAttributes().get(column);
            } else {
                return tupleRight.getAttributes().get(column);
            }
        } else {
            if (tupleLeft.getAttributes().containsKey(column) || tupleRight == null) {
                return tupleLeft.getAttributes().get(column);
            } else {
                return tupleRight.getAttributes().get(column);
            }
        }
    }

    public List<Tuple> Eval(List<Tuple> queryResult) throws Exception {
        //Select and Join expression evaluate.
        PrimitiveValue result = eval(this.expression);
        if (result.toBool()) {
            Tuple tuple = this.tupleRight != null ? this.tupleLeft.joinTuple(this.tupleRight) : this.tupleLeft;
            queryResult.add(tuple);
        }
        return queryResult;
    }


    public Tuple projectEval(List<TableObject> involvedTable) throws Exception {
        //todo 查表后进行 projection，优化，利用新列定义，不解析 selectItem
        Tuple newTuple = new Tuple();
        HashMap<Column, PrimitiveValue> attributes = new HashMap<>();
        for (int i = 0; i < selectList.size(); i++) {
            Object s = selectList.get(i);
            if (s instanceof AllColumns) {
                newTuple = tupleLeft;
                break;
            } else if (s instanceof AllTableColumns) {
                //todo
            } else {
                //  todo 优化
                if (((SelectExpressionItem) s).getExpression() instanceof Column) {
                    String alias = ((SelectExpressionItem) s).getAlias();
                    Column column = (Column) ((SelectExpressionItem) s).getExpression();
                    if (alias == null) {
                        attributes.put(column, tupleLeft.getAttributes().get(column));
                    } else {
                        Column newCol = new Column(column.getTable(), alias);
                        attributes.put(newCol, tupleLeft.getAttributes().get(column));
                    }
                } else {
                    PrimitiveValue result = eval(((SelectExpressionItem) s).getExpression());
                    String name = ((SelectExpressionItem) s).getAlias();
                    attributes.put(new Column(null, name), result);
                }
            }
            newTuple.setAttributes(attributes);
        }

        newTuple.setTableName(tupleLeft.getTableName());
        return newTuple;
    }

}
