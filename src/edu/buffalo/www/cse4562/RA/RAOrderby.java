package edu.buffalo.www.cse4562.RA;

import edu.buffalo.www.cse4562.Table.TableObject;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.List;

public class RAOrderby extends RANode {
    private String operation = "ORDERBY";
    private List orderby;

    public RAOrderby(List orderby) {
        this.orderby = orderby;
    }

    public List getOrderby() {
        return orderby;
    }

    public void setOrderby(List orderby) {
        this.orderby = orderby;
    }

    public String getOperation() {
        return operation;
    }

    public TableObject Eval(TableObject table){
        return table;
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
