package edu.buffalo.www.cse4562.RA;

import edu.buffalo.www.cse4562.Evaluate.evaluate;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.List;

public class RASelection extends RANode {

    private String operation = "SELECTION";
    private Expression where;

    public List<Tuple> Eval(List<Tuple> queryResult, Tuple tuple, TableObject tableLeft) throws Exception {
            Tuple tupleRight = null;
            evaluate eva = new evaluate(tuple, tupleRight, this.getWhere());
            if (eva.selectEval()) {
                queryResult.add(tuple);
            }
        return queryResult;
    }

    public RASelection(Expression where) {
        this.where = where;
    }

    public String getOperation() {
        return operation;
    }

    public Expression getWhere() {
        return where;
    }

    public void setWhere(Expression where) {
        this.where = where;
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
