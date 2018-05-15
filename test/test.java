
import edu.buffalo.www.cse4562.Table.Tuple;
//import edu.buffalo.www.cse4562.Table.Column;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;


import java.io.*;

public class test {
    public static void main(String[] args) throws Exception {

        Column a =  new Column(new Table(),"123");
        int i =1;

//        int i = 0;
    }

    public static void a(Tuple a) throws Exception {
        File file = new File("indexes/" + 1 + ".txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        FileOutputStream outputStream = new FileOutputStream(file, true);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(a);
        objectOutputStream.flush();
        objectOutputStream.close();
    }
}
