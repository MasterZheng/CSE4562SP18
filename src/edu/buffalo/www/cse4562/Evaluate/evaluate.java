package edu.buffalo.www.cse4562.Evaluate;

import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.sql.SQLException;
import java.util.HashMap;

public class evaluate extends Eval {
    private Tuple tupleLeft;
    private Tuple tupleRight;
    private Expression expression;

    private ColDataType changeType;

    public evaluate(Tuple tupleLeft, Tuple tupleRight, Expression expression) {
        this.tupleLeft = tupleLeft;
        this.tupleRight = tupleRight;
        this.expression = expression;
    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException {
        HashMap tupleMap = tupleLeft.getAttributes();
        String colName = column.getColumnName().toUpperCase();
        for (int i = 0; i < tupleLeft.getTableObject().getColumnDefinitions().size(); i++) {
            ColumnDefinition def = tupleLeft.getTableObject().getColumnDefinitions().get(i);
            if (def.getColumnName().equals(colName)) {
                changeType = def.getColDataType();
            }
        }
        PrimitiveValue Value;
        if (changeType.toString().toUpperCase().equals("INT") || changeType.toString().toUpperCase().equals("LONG")) {
            Value = new LongValue(tupleMap.get(colName).toString());
        } else if (changeType.toString().toUpperCase().equals("STRING")) {
            Value = new StringValue(tupleMap.get(colName).toString());
        } else {
            Value = new NullValue();
        }
        return Value;
    }

    public boolean processEval() throws Exception {
        PrimitiveValue result = eval(expression);
        return result.toBool();
    }

}
