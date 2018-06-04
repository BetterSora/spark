import java.util.ArrayList;

public class Test {
    private int num = 10;

    public static <T> ArrayList<T> func() {
        return new ArrayList<T>();
    }

    public static void main(String[] args) {
        System.out.println(Test.<String>func());
        int[] arr = {0, 1, 2, 3};

        for (int i : arr) {
            System.out.println(i);
        }

        synchronized ("1") {

        }

        new Test().func2(new Test());
    }

    public void func2(Test obj) {
        System.out.println(obj.num);
    }
}

abstract class Person {
    public abstract void doSomething();
}

class Studeng extends Person {

    @Override
    public void doSomething() {
    }
}
