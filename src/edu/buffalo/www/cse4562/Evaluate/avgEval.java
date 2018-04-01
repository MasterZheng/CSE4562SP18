package edu.buffalo.www.cse4562.Evaluate;

import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.Function;

import java.util.*;

public class avgEval {
    private HashMap<Integer, ArrayList<Tuple>> hashMap = new HashMap<>();
    private List<Function> functionList = new ArrayList<>();
    private int counter = 0;

    public avgEval(TableObject tableObject, List<Function> functionList) {
        this.hashMap = tableObject.getHashMap();
        this.functionList = functionList;
    }
    public HashMap<String[], ArrayList<Double>> eval(){
        HashMap<String[],ArrayList<Double>> result = new HashMap<>();
        Iterator iterator = hashMap.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry m= (Map.Entry)iterator.next();
            ArrayList<Double> resultList = new ArrayList<>();
            result.put((String[]) m.getKey(),resultList);
            for (int i = 0;i<functionList.size();i++){

            }
        }
        return result;
    }

}
