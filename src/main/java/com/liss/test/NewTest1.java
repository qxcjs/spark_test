package com.liss.test;

public class NewTest1 {
    public static String s1 = "static";  // 第一句

    public static void main(String[] args) {
        String s1 = new String("he") + new String("llo"); //第二句
        s1.intern();   // 第三句
        String s2 = "hello";  //第四句
        System.out.println(s1 == s2);//第五句，输出是true。
    }

}
