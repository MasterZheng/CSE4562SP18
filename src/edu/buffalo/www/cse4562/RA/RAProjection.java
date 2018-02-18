package edu.buffalo.www.cse4562.RA;

import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;

public class RAProjection extends RANode {

    private String operation = "PROJECTION";
    private List selectItem;

    public RAProjection(List<SelectItem> selectItem) {
        this.selectItem = selectItem;
    }

    public List getSelectItem() {
        return selectItem;
    }

    public void setSelectItem(List selectItem) {
        this.selectItem = selectItem;
    }

    public String getOperation() {
        return this.operation;
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
