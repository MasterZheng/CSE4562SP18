package edu.buffalo.www.cse4562.SQLparser;

import edu.buffalo.www.cse4562.Evaluate.expProcess;
import edu.buffalo.www.cse4562.RA.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class SelectParser {
    /*
    implement 2 ways to parse SQL
     */
    private SelectBody body;
    private List<Join> joins;
    private List<SelectItem> selectItem;
    private Expression where;
    private List orderBy;
    private Distinct dist;
    private Limit lim;
    private FromItem fromItem;

    public SelectParser(SelectBody body){
        this.body = body;
        this.joins = ((PlainSelect) body).getJoins();
        this.selectItem = ((PlainSelect) body).getSelectItems();
        this.where = ((PlainSelect) body).getWhere();
        this.orderBy = ((PlainSelect) body).getOrderByElements();
        this.dist = ((PlainSelect) body).getDistinct();
        this.lim = ((PlainSelect) body).getLimit();
        this.fromItem = ((PlainSelect) body).getFromItem();


    }
    static Logger logger = Logger.getLogger(SelectItem.class.getName());

    /**
     * Parse the SelectBody into HashMap
     *
     * @param body
     * @param tableCounter
     * @return
     */
    public static HashMap<String, Object> SelectFunction(SelectBody body, int tableCounter) {

        HashMap<String, Object> query = new HashMap<>();
        HashMap<String, Object> operations = new HashMap<>();

        if (!(body instanceof Union)) {
            // get required elements
            List<Join> joins = ((PlainSelect) body).getJoins();
            List<SelectItem> selectItem = ((PlainSelect) body).getSelectItems();
            Expression where = ((PlainSelect) body).getWhere();
            List orderby = ((PlainSelect) body).getOrderByElements();
            Distinct dist = ((PlainSelect) body).getDistinct();
            Limit lim = ((PlainSelect) body).getLimit();
            FromItem fromItem = ((PlainSelect) body).getFromItem();
            String tableAlias = fromItem.getAlias();

            tableCounter = solveFromList(query, fromItem, tableCounter, tableAlias);

            if (joins != null) {
                for (int i = 0; i < joins.size(); i++) {
                    tableCounter = solveFromList(query, joins.get(i).getRightItem(), tableCounter, joins.get(i).getRightItem().getAlias());
                }
            }

            if (where != null) {
                operations.put("WHERE", where);
            }

            //Select Item
            if (selectItem != null) {
                //The select list must be traversed during the data process
                //Do not need to parse the select list here,
                operations.put("SELECTITEM", selectItem);
            }

            if (orderby != null) {
                operations.put("ORDERBY", orderby);
            }
            if (dist != null) {
                operations.put("DISTINCT", dist);
            }
            if (lim != null) {
                operations.put("LIMIT", lim);
            }

            query.put("OPERATIONS", operations);
        } else {
            //todo UNION
            int unionSize = ((Union) body).getPlainSelects().size();
            for (int i = 0; i < unionSize; i++) {
                PlainSelect sub = ((Union) body).getPlainSelects().get(i);
                //query.put("Union",SelectFunction(sub,tableCounter));
            }
        }
        return query;
    }

    public static int solveFromList(HashMap<String, Object> query, FromItem fromItem, int tableCounter, String tableAlias) {
        if (fromItem instanceof SubSelect) {
            //1 subselect
            SubSelect sub = ((SubSelect) fromItem);
            String subAlias = sub.getAlias();
            if (subAlias == null) {
                query.put("FromsubSelect" + tableCounter, SelectFunction(sub.getSelectBody(), tableCounter));
            } else {
                query.put(subAlias, SelectFunction(sub.getSelectBody(), tableCounter));
            }
        } else {
            //0 subselect
            if (tableAlias == null) {
                query.put("fromSelect" + tableCounter++, fromItem);
            } else {
                query.put(tableAlias, fromItem);
                tableCounter++;
            }
        }
        return tableCounter;
    }

    /**
     * Parse the SelectBody into RA tree
     *
     * @param body
     * @return the root node
     */
    public RANode SelectFunction(SelectBody body) {

        logger.info("Use RATree to parse SQL");

        if (!(body instanceof Union)) {
            //parse the SQL and build the tree down to up
            //process fromItem joins
            RANode joinNode = new RAJoin(fromItem, this.joins);
            RANode pointer = joinNode;
            if (fromItem instanceof SubSelect) {
                // subSelect
                RANode subSelect = SelectFunction(((SubSelect) fromItem).getSelectBody());
                joinNode.setLeftNode(subSelect);
                subSelect.setParentNode(pointer);
            } else {
                //table
                RATable table = new RATable((Table) fromItem);
                joinNode.setLeftNode(table);
                table.setParentNode(pointer);
            }

            if (joins != null) {
                for (int i = 0; i < joins.size(); i++) {
                    FromItem join = joins.get(i).getRightItem();

                    if (join instanceof SubSelect) {
                        if (joinNode.getRightNode() == null) {
                            RANode subSelect = SelectFunction(((SubSelect) join).getSelectBody());
                            joinNode.setRightNode(subSelect);
                            subSelect.setParentNode(pointer);
                        } else {
                            //if both children are not empty, new a new RAjoin node , insert the node into leftchild position
                            RANode joinNew = new RAJoin(fromItem, joins);
                            while (joinNode.getLeftNode() != null) {
                                joinNode = joinNode.getLeftNode();
                            }
                            pointer.setParentNode(joinNew);
                            joinNew.setLeftNode(pointer);
                            pointer = joinNew;
                            RANode subSelect = SelectFunction(((SubSelect) join).getSelectBody());
                            pointer.setRightNode(subSelect);
                            subSelect.setParentNode(pointer);

                        }
                    } else {
                        if (joinNode.getRightNode() == null) {
                            RATable table = new RATable((Table) join);
                            joinNode.setRightNode(table);
                            table.setParentNode(pointer);
                        } else {
                            //if both children are not empty, new a new RAjoin node , insert the node into leftchild position
                            RANode joinNew = new RAJoin(fromItem, joins);
                            while (joinNode.getLeftNode() != null) {
                                joinNode = joinNode.getLeftNode();
                            }
                            pointer.setParentNode(joinNew);
                            joinNew.setLeftNode(pointer);
                            pointer = joinNew;
                            RATable table = new RATable((Table) join);
                            pointer.setRightNode(table);
                            table.setParentNode(pointer);
                        }

                    }
                }
            }
            if (where!=null&&joins!=null){
                optimize(pointer,where);
            }
            //process where
            if (where != null) {
                RANode whereNode = new RASelection(where);
                whereNode.setLeftNode(pointer);
                pointer.setParentNode(whereNode);
                pointer = whereNode;
            } else {
                // if no where ,add 1==1 as the where expression
                EqualsTo rightWhere = new EqualsTo(new LongValue(1), new LongValue(1));
                RANode whereNode = new RASelection(rightWhere);
                whereNode.setLeftNode(pointer);
                pointer.setParentNode(whereNode);
                pointer = whereNode;
            }

            //process projection
            RANode projNode = new RAProjection(selectItem);
            projNode.setLeftNode(pointer);
            pointer.setParentNode(projNode);
            pointer = projNode;

            //process orderby
            if (orderBy != null) {
                RANode orderbyNode = new RAOrderby(orderBy);
                orderbyNode.setLeftNode(pointer);
                pointer.setParentNode(orderbyNode);
                pointer = orderbyNode;
            }

            //process distinct
            if (dist != null) {
                RANode distNode = new RADistinct(dist);
                distNode.setLeftNode(pointer);
                pointer.setParentNode(distNode);
                pointer = distNode;
            }

            //process limit
            if (lim != null) {
                RANode limNode = new RALimit(lim);
                limNode.setLeftNode(pointer);
                pointer.setParentNode(limNode);
                pointer = limNode;
            }
            return pointer;
        } else {
            //todo UNION
            return null;
        }
    }

    public boolean optimize(RANode pointer,Expression where){
        //存在 join 情况
        Expression newWhere = null;
        expProcess exp = new expProcess(where);
        List<Expression> expList = exp.getExpressions();
        boolean flag =false;
        List<Integer> deleteExp = new ArrayList<>();
        for (int i = 0;i<expList.size();i++){
            flag = false;
            if (pointer.getRightNode() instanceof RATable){
                flag = exp.isRelated(((RATable) pointer.getRightNode()).getTable(),expList.get(i));
            }else if (pointer.getRightNode() instanceof RAJoin){
                flag = optimize(pointer.getRightNode(),expList.get(i));
            }
            if (pointer.getLeftNode() instanceof RATable){
                flag = flag||exp.isRelated(((RATable) pointer.getLeftNode()).getTable(),expList.get(i));
            }else if (pointer.getLeftNode() instanceof RAJoin){
                flag = flag||optimize(pointer.getLeftNode(),expList.get(i));
            }
            if (flag){
                deleteExp.add(i);
                ((RAJoin)pointer).addAndExpression(expList.get(i));
            }
        }

        for (int i = deleteExp.size()-1;i>-1;i--){//从大往小删
            expList.remove(i);
        }
        for (Expression e:expList){
            newWhere = exp.mergeAndExpression(newWhere,e);
        }
        this.where = newWhere;
        return flag;
    }
}
