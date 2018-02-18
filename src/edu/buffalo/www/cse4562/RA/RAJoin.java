package edu.buffalo.www.cse4562.RA;

import net.sf.jsqlparser.statement.select.FromItem;

import java.util.List;

public class RAJoin extends RANode {
    private String operation = "JOIN";
    private FromItem fromItem;
    private List join;

    public RAJoin(FromItem fromItem, List join) {
        this.fromItem = fromItem;
        this.join = join;
    }

    public FromItem getFromItem() {
        return fromItem;
    }

    public void setFromItem(FromItem fromItem) {
        this.fromItem = fromItem;
    }

    public List getJoin() {
        return join;
    }

    public void setJoin(List join) {
        this.join = join;
    }

    public boolean hasNext() {
        if (this.leftNode != null || this.rightNode != null) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public RANode next() {
        //todo
        return this.leftNode;
    }
}
