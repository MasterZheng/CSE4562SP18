package edu.buffalo.www.cse4562.RA;

import net.sf.jsqlparser.statement.select.Distinct;

public class RADistinct extends RANode {
    private String operation = "DISTINCT";
    private Distinct distinct;

    public RADistinct(Distinct distinct) {
        this.distinct = distinct;
    }

    public Distinct getDistinct() {
        return distinct;
    }

    public void setDistinct(Distinct distinct) {
        this.distinct = distinct;
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
