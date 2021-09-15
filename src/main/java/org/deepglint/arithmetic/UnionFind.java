package org.deepglint.arithmetic;

/**
 * 并查集接口
 *
 * @author ZhangFuQi
 * @date 2021/9/8 9:01
 */
public interface UnionFind {
    /**
     * 获取并查集大小
     *
     * @return 大小
     */
    int getSize();

    /**
     * 判断是否链接
     *
     * @param p p集合
     * @param q q集合
     * @return boolean false表示没有链接，true表示已连接
     */
    boolean isConnected(int p, int q);

    /**
     * 链接两个集合
     *
     * @param p p集合
     * @param q q集合
     */
    void unionElement(int p, int q);

    /**
     * 查找对应的元素
     *
     * @param p 元素位置
     * @return 元素值
     */
    int find(int p);
}
