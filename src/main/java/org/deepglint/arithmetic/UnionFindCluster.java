package org.deepglint.arithmetic;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.util.MathUtils;
import org.deepglint.model.Person;
import org.deepglint.model.RawData;
import org.deepglint.util.FeatureDecoder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 并查集聚类算法
 *
 * @author ZhangFuQi
 * @date 2021/9/8 8:56
 */
public class UnionFindCluster<T extends RawData> extends Cluster<T> implements UnionFind {
    public static final float THRESHOLD = 0.5f;
    private int[] id;

    public UnionFindCluster() {
        super();
    }

    public UnionFindCluster(int size) {
        super();
        id = new int[size];
        for (int i = 0; i < size; i++) {
            id[i] = i;
        }
    }

    public List<Cluster<T>> cluster(final List<T> points) {
        MathUtils.checkNotNull(points);
        final List<Cluster<T>> clusters = new ArrayList<>();
        for (int i = 0; i < points.size(); i++) {
            for (int j = 0; j < points.size(); j++) {
                if (cosineSimilarity(points.get(i), points.get(j))) {
                    if (!isConnected(i, j)) {
                        unionElement(i, j);
                    }
                }
            }
        }
        Set<Integer> set = new HashSet<>();
        for (int j : id) {
            set.add(j);
        }
        System.out.println(set.size());

        for (Integer integer : set) {
            Cluster<T> cluster = new Cluster<>();
            for (int i = 0; i < id.length; i++) {
                if (id[i] == integer) {
                    cluster.addPoint(points.get(i));
                }
            }
            clusters.add(cluster);
        }
        return clusters;
    }

    private boolean cosineSimilarity(T p, T q) {
        List<Float> a = FeatureDecoder.decode(p.getFeature());
        List<Float> b = FeatureDecoder.decode(q.getFeature());
        float ans = 0;
        for (int i = 0; i < a.size(); i++) {
            ans += a.get(i) * b.get(i);
        }
        return ans > THRESHOLD;
    }

    @Override
    public void addPoint(T point) {
        super.addPoint(point);
    }

    @Override
    public List<T> getPoints() {
        return super.getPoints();
    }

    @Override
    public int getSize() {
        return id.length;
    }

    @Override
    public boolean isConnected(int p, int q) {
        return find(p) == find(q);
    }

    public void unionElement(int p, int q, List<Person> personList) {
        int pId = find(p);
        int qId = find(q);
        if (pId == qId) {
            return;
        }
        for (int i = 0; i < id.length; i++) {
            if (id[i] == pId) {
                id[i] = qId;
                personList.get(i).setPersonId(personList.get(qId).getPersonId());
            }
        }
    }

    @Override
    public void unionElement(int p, int q) {
        int pId = find(p);
        int qId = find(q);
        if (pId == qId) {
            return;
        }
        for (int i = 0; i < id.length; i++) {
            if (id[i] == pId) {
                id[i] = qId;
            }
        }
    }

    @Override
    public int find(int p) {
        if (p < 0 || p >= id.length) {
            throw new IllegalArgumentException("p is out of bound!");
        }
        return id[p];
    }
}
