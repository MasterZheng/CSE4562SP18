package edu.buffalo.www.cse4562.Evaluate;

import edu.buffalo.www.cse4562.Table.TempTable;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;

import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;
import java.util.HashMap;

public class evaluate extends Eval {
    private Tuple tupleLeft;
    private Tuple tupleRight;
    private Expression expression;

    public evaluate(Tuple tupleLeft, Tuple tupleRight, Expression expression) {
        this.tupleLeft = tupleLeft;
        this.tupleRight = tupleRight;
        this.expression = expression;
    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException {
        HashMap tupleMap = tupleLeft.getAttributes();
        tupleMap.get(column.getColumnName().toUpperCase());
        //todo modify the type
        StringValue val = new StringValue(tupleMap.get(column.getColumnName().toUpperCase()).toString()) ;
        return val;
    }

    public boolean processEval()throws Exception{

        PrimitiveValue result =  eval(expression);
        return result.toBool();
    }

}
