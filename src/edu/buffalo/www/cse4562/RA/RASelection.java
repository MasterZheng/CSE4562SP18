package edu.buffalo.www.cse4562.RA;

import net.sf.jsqlparser.expression.Expression;

public class RASelection extends RANode {

    private String operation = "SELECTION";
    private Expression where;

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
        return false;
    }

    @Override
    public Object next() {
        return null;
    }
}
