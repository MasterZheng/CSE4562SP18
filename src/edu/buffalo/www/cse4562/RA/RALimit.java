package edu.buffalo.www.cse4562.RA;

import net.sf.jsqlparser.statement.select.Limit;

public class RALimit extends RANode {

    private String operation = "LIMIT";
    private Limit limit;

    public RALimit(Limit limit) {
        this.limit = limit;
    }

    public Limit getLimit() {
        return limit;
    }

    public void setLimit(Limit limit) {
        this.limit = limit;
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
    public RANode next() {
        return this.leftNode;
    }
}
