import java.io.Serializable;
import java.util.Vector;

public class Page {
    private Vector<Tuple> tuples;

    public Vector<Tuple> getTuples() {
        return tuples;
    }
    public void setTuples(Vector<Tuple> tuples) {
        this.tuples = tuples;
    }

    public String toString(){
        String res = "";
        for(int i = 0; i < tuples.size(); i++){
            res += tuples.get(i).toString() + ",";
        }
        return res.substring(0,res.length() - 1);
    }
    public static void main(String[] args){
        Page p = new Page();
        Tuple v1 = new Tuple("Yousef", 20, "zdfg");
        Tuple v2 = new Tuple("Seif", 19, "xcfhbf");
        
        p.tuples = new Vector<Tuple>();
        p.tuples.add(v1);
        p.tuples.add(v2);
        System.out.print(p);
    }
}

class Tuple implements Serializable{
    private String name;
    private Integer age;
    private String address;

    public Tuple(String name, Integer age, String address){
        this.name = name;
        this.age = age;
        this.address = address;
    }

    public String getName(){
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return name + "," + age + "," + address;
    }
}
