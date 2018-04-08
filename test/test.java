import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import java.util.ArrayList;
import java.util.List;

public class test {
    public static void main(String[] args){
        List<Expression> e = new ArrayList<>();
        OrExpression a2 = new OrExpression(new EqualsTo(new LongValue(1), new LongValue(1)),new EqualsTo(new LongValue(2), new LongValue(2)));
        AndExpression a1= new AndExpression(null,a2);
        selectionEval x = new selectionEval(a1);
        x.parse2List(e,a1);
    }
}
