package edu.buffalo.www.cse4562.Table;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.Comparator;
import java.util.List;

public class TupleComparator implements Comparator<Tuple> {
    private List<OrderByElement> orderByElements;

    public TupleComparator(List<OrderByElement> orderByElements) {
        this.orderByElements = orderByElements;
    }

    @Override
    public int compare(Tuple left, Tuple right) {
        if (orderByElements == null) return 0;

        for (OrderByElement element : orderByElements) {
            Column column = ((Column) element.getExpression());
            PrimitiveValue leftValue = left.getAttributes().get(column);
            PrimitiveValue rightValue = right.getAttributes().get(column);

            try {
                if (leftValue instanceof LongValue) {
                    if (leftValue.toLong() > rightValue.toLong()) {
                        return element.isAsc() ? 1 : -1;
                    } else if (leftValue.toLong() < rightValue.toLong()) {
                        return element.isAsc() ? -1 : 1;
                    }
                } else if (leftValue instanceof StringValue) {
                    if (leftValue.toString().compareTo(rightValue.toString()) > 0) {
                        return element.isAsc() ? 1 : -1;
                    } else if (leftValue.toString().compareTo(rightValue.toString()) < 0) {
                        return element.isAsc() ? -1 : 1;
                    }
                } else if (leftValue instanceof DoubleValue) {
                    if (leftValue.toDouble() > rightValue.toDouble()) {
                        return element.isAsc() ? 1 : -1;
                    } else if (leftValue.toDouble() < rightValue.toDouble()) {
                        return element.isAsc() ? -1 : 1;
                    }
                } else if (leftValue instanceof DateValue) {
                    if (((DateValue) leftValue).getValue().getTime() > ((DateValue) rightValue).getValue().getTime()) {
                        return element.isAsc() ? 1 : -1;
                    } else if (((DateValue) leftValue).getValue().getTime() < ((DateValue) rightValue).getValue().getTime()) {
                        return element.isAsc() ? -1 : 1;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return 0;
    }
}
