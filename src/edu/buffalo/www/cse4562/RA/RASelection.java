package edu.buffalo.www.cse4562.RA;

import edu.buffalo.www.cse4562.Evaluate.evaluate;
import edu.buffalo.www.cse4562.Evaluate.expProcess;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RASelection extends RANode {

    private String operation = "SELECTION";
    private Expression expression;


    public RASelection(Expression where) {
        this.expression = where;
    }

    public String getOperation() {
        return operation;
    }

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(Expression where) {
        this.expression = where;
    }

    @Override
    public boolean hasNext() {
        if (this.leftNode != null) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Object next() {
        return this.leftNode;
    }
}
