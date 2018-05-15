import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class test {
    public static void main(String[] args) throws Exception {
        String[] strs = {"1","3","12","33"};
        String ids="1,2,33,45,67";
        List<String> sList = Arrays.asList(strs);
        List<String> idsStringList = Arrays.asList(ids.split(","));
        List<Integer> idsList = new ArrayList<>();
    }}
