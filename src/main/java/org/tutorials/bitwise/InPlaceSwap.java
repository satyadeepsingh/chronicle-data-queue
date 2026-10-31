package org.tutorials.bitwise;

public class InPlaceSwap {

    public static void main(String[] args) {
        int x = 5, y = 6;
        inplaceSwap(x, y);
    }

    private static void inplaceSwap(int x, int y) {
        y = x ^ y;
        x = x ^ y;
        y = x ^ y;
        System.out.println("in-place swap for (x = 5, y = 6) is x = " + x + ", y = " + y);
    }
}
