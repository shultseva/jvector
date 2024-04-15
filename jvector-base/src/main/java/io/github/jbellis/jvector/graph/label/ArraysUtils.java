package io.github.jbellis.jvector.graph.label;

import io.github.jbellis.jvector.util.BitSet;
import io.github.jbellis.jvector.util.FixedBitSet;

import java.util.Arrays;

public class ArraysUtils {

    public static boolean hasCommon(int[] a, int[] b) {
        int bLen = b.length;
        int aLen = a.length;
        int i = 0;
        int j = 0;
        while (i < aLen && j < bLen) {
            if (a[i] == b[j]) {
                return true;
            }
            if (a[i] < b[j]) {
                var idx = Arrays.binarySearch(b, j, bLen, a[i]);
                if (idx >= 0) {
                    return true;
                }
                j = -idx - 1;
                i++;
            } else if (a[i] > b[j]) {
                var idx = Arrays.binarySearch(a, i, aLen, b[j]);
                if (idx >= 0) {
                    return true;
                }
                i = -idx - 1;
                j++;
            }
        }
        return false;
    }


    public static FixedBitSet toBitSet(int[] input) {
        var set = new FixedBitSet(input.length);
        for (var in : input) {
            set.set(in);
        }
        return set;
    }
}
