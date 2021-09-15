package org.deepglint.util;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class FeatureDecoder {
    public static List<Float> decode(String featureStr) {
        List<Float> feature = new ArrayList<>();
        byte[] bs = Base64.getDecoder().decode(featureStr);
        for (int i = 0; i < bs.length; i += 4) {
            feature.add(byte2float(bs, i));
        }
        return feature;
    }

    private static float byte2float(byte[] b, int index) {
        int l;
        l = b[index];
        l &= 0xff;
        l |= ((long) b[index + 1] << 8);
        l &= 0xffff;
        l |= ((long) b[index + 2] << 16);
        l &= 0xffffff;
        l |= ((long) b[index + 3] << 24);
        return Float.intBitsToFloat(l);
    }
}
