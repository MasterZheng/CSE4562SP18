package edu.buffalo.www.cse4562.RA;

import edu.buffalo.www.cse4562.Evaluate.evaluate;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.schema.Table;
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

//    public List<Tuple> Eval(List<Tuple> queryResult,List<ColumnDefinition> columnDefinitions,ArrayList<TableObject> tableList)throws Exception{
//        List<Tuple> result = new ArrayList<>();
//
//        for(int i = 0;i<queryResult.size();i++){
//            evaluate eva = new evaluate(queryResult.get(i),this.selectItem,tableList);
//            result.add(eva.projectEval(columnDefinitions));
//        }
//        return result;
//    }
    public TableObject Eval(TableObject OutputTable,String tableName)throws Exception{
        List<Tuple> result = new ArrayList<>();
        Table table = new Table(tableName);
        for(int i = 0;i<OutputTable.getTupleList().size();i++){
            evaluate eva = new evaluate(OutputTable.getTupleList().get(i),this.selectItem);
            result.add(eva.projectEval(OutputTable.getColumnInfo(),table));
        }
        OutputTable.settupleList(result);
        return OutputTable;
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
