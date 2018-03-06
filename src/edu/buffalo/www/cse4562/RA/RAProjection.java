package edu.buffalo.www.cse4562.RA;

import edu.buffalo.www.cse4562.Evaluate.evaluate;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.HashMap;
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

    public List<Tuple> Eval(List<Tuple> queryResult,List<ColumnDefinition> columnDefinitions,HashMap<String, TableObject> tableMap)throws Exception{
        List<Tuple> result = new ArrayList<>();

        for(int i = 0;i<queryResult.size();i++){
            evaluate eva = new evaluate(queryResult.get(i),this.selectItem,tableMap);
            result.add(eva.projectEval(columnDefinitions));
        }
        return result;
    }
    public TableObject Eval(TableObject tableObject,HashMap<String, TableObject> tableMap)throws Exception{
        List<Tuple> result = new ArrayList<>();

        for(int i = 0;i<tableObject.getTupleList().size();i++){
            evaluate eva = new evaluate(tableObject.getTupleList().get(i),this.selectItem,tableMap);
            result.add(eva.projectEval(tableObject.getColumnDefinitions()));
        }
        tableObject.settupleList(result);
        return tableObject;
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
