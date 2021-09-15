package org.deepglint.util;

import java.util.UUID;

/**
 * Id工具类
 *
 * @author ZhangFuQi
 * @date 2021/9/10 9:06
 */
public class IdUtils {
    public static String getRandomId() {
        return UUID.randomUUID().toString();
    }
}
