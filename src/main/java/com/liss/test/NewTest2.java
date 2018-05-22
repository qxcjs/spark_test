package com.liss.test;

public class NewTest2 {
    public static void main(String[] args) {
        String s1 = new String("he") + new String("llo"); // ①
        String s2 = new String("h") + new String("ello"); // ②
        String s3 = s1.intern(); // ③
        String s4 = s2.intern(); // ④
        System.out.println(s1 == s3);
        System.out.println(s1 == s4);
    }

}
