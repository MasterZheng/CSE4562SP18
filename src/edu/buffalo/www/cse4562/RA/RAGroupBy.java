package edu.buffalo.www.cse4562.RA;

import edu.buffalo.www.cse4562.Evaluate.projectionEval;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RAGroupBy extends RANode {
    private List<Column> groupByReferences;
    private String operation = "GROUPBY";
    private List<Function> aggSelect = new ArrayList<>();

    public RAGroupBy(List<Column> groupByReferences, List<SelectItem> selectItems) {
        this.aggSelect = projectionEval.extractFunc(selectItems);
        this.groupByReferences = groupByReferences;
    }


    public List<Column> getGroupByReferences() {
        return groupByReferences;
    }

    public TableObject Eval(TableObject OutputTable, List<Column> groupByReferences) throws Exception {
        List<Tuple> originalList = OutputTable.getTupleList();
        HashMap<Integer, ArrayList<Tuple>> hashMap = OutputTable.getHashMap();
        for (int i = 0;i<originalList.size();i++){
            Tuple t = originalList.get(i);
            String hashCode = "";
            int key = 0;
            for (int j = 0;j<groupByReferences.size();j++){
                hashCode+=t.getAttributes().get(groupByReferences.get(j)).toRawString();
                key = hashCode.hashCode();
            }
            if (hashMap.containsKey(key)){
                hashMap.get(key).add(t);
            }else {
                ArrayList<Tuple> grouplist = new ArrayList<>();
                grouplist.add(t);
                hashMap.put(key,grouplist);
            }
        }
        OutputTable.settupleList(new ArrayList<>());
        return OutputTable;
    }

    public List<Function> getAggSelect() {
        return aggSelect;
    }



    @Override
    public String getOperation() {
        return operation;
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
        return null;
    }
}
