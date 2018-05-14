
import edu.buffalo.www.cse4562.Table.Tuple;
//import edu.buffalo.www.cse4562.Table.Column;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;


import java.io.*;

public class test {
    public static void main(String[] args) throws Exception {

        Column a =  new Column(new Table(),"123");
        edu.buffalo.www.cse4562.Table.Column B = (edu.buffalo.www.cse4562.Table.Column)a;
        int i =1;
//        Tuple a = new Tuple();
//        a.getAttributes().put(new Column(null,"123"), new DoubleValue(1));
//        a(a);
//        FileInputStream inputStream = new FileInputStream(new File("indexes/" + 1 + ".txt"));//创建文件字节输出流对象
//        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
//        Tuple temp = (Tuple) objectInputStream.readObject();
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
