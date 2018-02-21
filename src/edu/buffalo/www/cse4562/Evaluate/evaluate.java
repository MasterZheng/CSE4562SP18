package edu.buffalo.www.cse4562.Evaluate;

import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class evaluate extends Eval {
    static Logger logger = Logger.getLogger(evaluate.class.getName());
    private Tuple tupleLeft;
    private Tuple tupleRight;
    private Expression expression;
    private HashMap<String, TableObject> tableMap;
    private List<Object> selectList;

    public evaluate(Tuple tupleLeft, Tuple tupleRight, Expression expression,HashMap<String, TableObject> tableMap) {
        this.tupleLeft = tupleLeft;
        this.tupleRight = tupleRight;
        this.expression = expression;
        this.tableMap = tableMap;
    }

    public evaluate(Tuple tupleLeft, List<Object> list) {
        this.tupleLeft = tupleLeft;
        this.selectList = list;
    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException {
        ColDataType changeType = null;

        //according to the column name , compare the ColumnDefinitions to determine the type.
        //and get the value by column name
        HashMap tupleMap = tupleLeft.getAttributes();
        String tableName = tupleLeft.getTableName();
        String colName = column.getColumnName().toUpperCase();
        List<ColumnDefinition> columnDefinitions = tableMap.get(tableName).getColumnDefinitions();
        for (int i = 0; i < columnDefinitions.size(); i++) {
            ColumnDefinition def = columnDefinitions.get(i);
            if (def.getColumnName().equals(colName)) {
                changeType = def.getColDataType();
                break;
            }
        }
        PrimitiveValue Value;
        //todo modify tuple map
        if (changeType.toString().toUpperCase().equals("INT") || changeType.toString().toUpperCase().equals("LONG")) {
            Value = new LongValue(tupleMap.get(colName).toString());
        } else if (changeType.toString().toUpperCase().equals("STRING")) {
            Value = new StringValue(tupleMap.get(colName).toString());
        } else if (changeType.toString().toUpperCase().equals("DATE")) {
            Value = new DateValue(tupleMap.get(colName).toString());
        } else {
            Value = new NullValue();
        }
        return Value;
    }

    public boolean selectEval() throws Exception {
        PrimitiveValue result = eval(expression);
        return result.toBool();
    }

    public Tuple projectEval(List<ColumnDefinition> list) throws Exception {
        Tuple newTuple = new Tuple();
        HashMap<String, Object> attributes = new HashMap<>();
        for (Object s : selectList) {
            if (s instanceof AllColumns) {
//                for (ColumnDefinition c : list) {
//                    String name = c.getColumnName();
//                    attributes.put(name, tupleLeft.getAttributes().get(name));
//                }
//                newTuple.setAttributes(attributes);
                newTuple = tupleLeft;
            } else if (s instanceof AllTableColumns){
                //todo
            } else if (((SelectExpressionItem) s).getExpression() instanceof Column) {
                String name = ((Column) ((SelectExpressionItem) s).getExpression()).getColumnName();
                String alias = ((SelectExpressionItem) s).getAlias();
                if (alias ==null){
                    attributes.put(name, tupleLeft.getAttributes().get(name));
                }else {
                    attributes.put(alias, tupleLeft.getAttributes().get(name));
                }

                newTuple.setAttributes(attributes);
            } else {
                PrimitiveValue result = eval(((SelectExpressionItem) s).getExpression());
                String name = ((SelectExpressionItem) s).getAlias();
                attributes.put(name, result);
                newTuple.setAttributes(attributes);
            }

        }

        return newTuple;
    }

}
