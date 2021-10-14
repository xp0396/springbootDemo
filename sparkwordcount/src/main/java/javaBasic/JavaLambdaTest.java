package javaBasic;
/*
Lambda表达式主要用于定义一个函数式接口（functional interface：一个只包含一个抽象方法的接口）的内联实现，在上面的例子中，
我们使用了各种类型的Lambda表达式来实现MathOperation接口的operation方法，接着又实现了GreetingService接口的sayMessage方法，
Runnable接口的run方法； Lambda表达式消除了匿名类的使用并且赋予Java简单且强大的函数式编程能力
*/

public class JavaLambdaTest {
    public static void main(String args[])
    {
        JavaLambdaTest tester = new JavaLambdaTest();

        // 有参数类型
        MathOperation addition = (int a, int b) -> a + b;

        // 无参数类型
        MathOperation subtraction = (a, b) -> a - b;

        // 有花括号，有return关键字
        MathOperation multiplication = (int a, int b) -> {
            return a * b;
        };

        // 无花括号，无return关键字，单一表达式情况
        MathOperation division = (int a, int b) -> a / b;

        // MathOperation调用示例
        System.out.println("10 + 5 = " + tester.operate(10, 5, addition));
        System.out.println("10 - 5 = " + tester.operate(10, 5, subtraction));
        System.out.println("10 x 5 = " + tester.operate(10, 5, multiplication));
        System.out.println("10 / 5 = " + tester.operate(10, 5, division));

        // 有括号
        GreetingService greetService1 = message -> System.out.println("Hello " + message);

        // 无括号，单个参数情况
        GreetingService greetService2 = (message) -> System.out.println("Hello " + message);

        // GreetingService调用示例
        greetService1.sayMessage("Mahesh");
        greetService2.sayMessage("Suresh");

        //有括号， 无参情况
        Runnable runTest = () -> System.out.println("Running");
        //Runnable调用示例
        runTest.run();
    }

    // 内部接口
    interface MathOperation
    {
        int operation(int a, int b);
    }

    interface GreetingService
    {
        void sayMessage(String message);
    }

    interface Runnable
    {
        void run();
    }

    private int operate(int a, int b, MathOperation mathOperation)
    {
        return mathOperation.operation(a, b);
    }
}
