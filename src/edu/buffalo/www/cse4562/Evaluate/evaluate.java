package edu.buffalo.www.cse4562.Evaluate;

import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class evaluate extends Eval {
    private Tuple tupleLeft;
    private Tuple tupleRight;
    private Expression expression;
    private List<SelectExpressionItem> selectList;
    private ColDataType changeType;

    public evaluate(Tuple tupleLeft, Tuple tupleRight, Expression expression) {
        this.tupleLeft = tupleLeft;
        this.tupleRight = tupleRight;
        this.expression = expression;
    }

    public evaluate(Tuple tupleLeft,List<SelectExpressionItem> list){
        this.tupleLeft = tupleLeft;
        this.selectList = list;
    }
    @Override
    public PrimitiveValue eval(Column column) throws SQLException {
        HashMap tupleMap = tupleLeft.getAttributes();
        String colName = column.getColumnName().toUpperCase();
        for (int i = 0; i < tupleLeft.getColumnDefinitions().size(); i++) {
            ColumnDefinition def = tupleLeft.getColumnDefinitions().get(i);
            if (def.getColumnName().equals(colName)) {
                changeType = def.getColDataType();
            }
        }
        PrimitiveValue Value;
        if (changeType.toString().toUpperCase().equals("INT") || changeType.toString().toUpperCase().equals("LONG")) {
            Value = new LongValue(tupleMap.get(colName).toString());
        } else if (changeType.toString().toUpperCase().equals("STRING")) {
            Value = new StringValue(tupleMap.get(colName).toString());
        } else if (changeType.toString().toUpperCase().equals("DATE")){
            Value = new DateValue(tupleMap.get(colName).toString());
        }else {
            Value = new NullValue();
        }
        return Value;
    }

    public boolean whereEval() throws Exception {
        PrimitiveValue result = eval(expression);
        return result.toBool();
    }
    public Tuple selectEval(List<ColumnDefinition> list) throws Exception {
        Tuple newTuple = new Tuple();
        newTuple.setColumnDefinitions(list);
        HashMap<String,Object> attributes = new HashMap<>();
        for (SelectExpressionItem s:selectList){
            PrimitiveValue result = eval(s.getExpression());
            String name =null;
            if (s.getExpression() instanceof Column){
                name = ((Column)s.getExpression()).getColumnName();
            }else if (s.getExpression() instanceof Expression){
                name = s.getAlias();
            }
            attributes.put(name,result);
            newTuple.setAttributes(attributes);
        }
        return newTuple;
    }

}
