package edu.buffalo.www.cse4562.RA;

import edu.buffalo.www.cse4562.Evaluate.evaluate;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RASelection extends RANode {

    private String operation = "SELECTION";
    private Expression where;

    public List<Tuple> Eval(List<Tuple> queryResult, Tuple tupleLeft, Tuple tupleRight,ArrayList<TableObject> tableList) throws Exception {
        evaluate eva = new evaluate(tupleLeft, tupleRight, this.getWhere(),tableList);
            if (eva.selectEval()) {
                Tuple joinTuple = tupleRight!=null?tupleLeft.joinTuple(tupleRight):tupleLeft;
                queryResult.add(joinTuple);
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
