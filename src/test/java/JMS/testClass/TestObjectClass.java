package JMS.testClass;

public class TestObjectClass implements java.io.Serializable{
    private int number;
    private String name;

    public TestObjectClass(int number, String name) {
        this.number = number;
        this.name = name;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "TestObjectClass{" +
                "number=" + number +
                ", name='" + name + '\'' +
                '}';
    }
}
