package org.deepglint.arithmetic;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.deepglint.model.Person;
import org.deepglint.model.PersonChangeInfo;
import org.deepglint.model.Tags;
import org.deepglint.util.FeatureDecoder;

import java.io.Serializable;
import java.util.*;

/**
 * 并查集
 *
 * @author ZhangFuQi
 * @date 2021/9/8 11:53
 */
public class UnionSet<T extends Person> implements Serializable {
    public static final float THRESHOLD = 0.5f;
    private Map<String, String> personIdChangeMap = new HashMap<>();

    private final Map<T, Node<T>> nodeMap = new HashMap<>();
    private final Map<Node<T>, Node<T>> parentMap = new HashMap<>();
    private final Map<Node<T>, Integer> leaderSizeMap = new HashMap<>();

    public UnionSet() {
    }

    public UnionSet(List<T> dataList) {
        for (T data : dataList) {
            Node<T> node = new Node<T>(data);
            nodeMap.put(data, node);
            parentMap.put(node, node);
            leaderSizeMap.put(node, 1);
        }
    }

    private Node<T> findLeaderNode(Node<T> node) {
        Deque<Node<T>> path = new ArrayDeque<>();
        while (parentMap.get(node) != node) {
            path.push(node);
            node = parentMap.get(node);
        }
        while (!path.isEmpty()) {
            parentMap.put(path.pop(), node);
        }

        return node;
    }

    public T findLeader(T data) {
        Node<T> node = nodeMap.get(data);
        Deque<Node<T>> path = new ArrayDeque<>();
        while (parentMap.get(node) != node) {
            path.push(node);
            node = parentMap.get(node);
        }
        while (!path.isEmpty()) {
            parentMap.put(path.pop(), node);
        }
        return node.value;
    }


    public boolean isSameSet(T dataA, T dataB) {
        if (!nodeMap.containsKey(dataA) || !nodeMap.containsKey(dataB)) {
            return false;
        }

        return findLeaderNode(nodeMap.get(dataA)) == findLeaderNode(nodeMap.get(dataB));
    }

    public String addElement(T data, ProcessFunction.Context ctx) {
        Node<T> node = new Node<>(data);
        nodeMap.put(data, node);
        parentMap.put(node, node);
        leaderSizeMap.put(node, 1);
        Set<T> fit = new HashSet<>();
        for (Map.Entry<T, Node<T>> nodeEntry : nodeMap.entrySet()) {
            if (cosineSimilarity(data, nodeEntry.getKey())) {
                fit.add(findLeader(nodeEntry.getKey()));
            }
        }
        Iterator<T> itr = fit.iterator();
        T prev = itr.next();
        if (fit.size() == 1) {
            return prev.getPersonId();
        }
        while (itr.hasNext()) {
            T next = itr.next();
            prev = unionSet(prev, next);
        }
        for (T t : fit) {
            if (t.getPersonId().equals(prev.getPersonId())) {
                continue;
            }
            ctx.output(Tags.CHANGE_LIST_TAG, new PersonChangeInfo(t.getPersonId(), prev.getPersonId()));
        }

        return findLeader(data).getPersonId();
    }

    public T unionSet(T dataA, T dataB) {
        if (!nodeMap.containsKey(dataA) || !nodeMap.containsKey(dataB)) {
            return null;
        }

        Node<T> aHead = findLeaderNode(nodeMap.get(dataA));
        Node<T> bHead = findLeaderNode(nodeMap.get(dataB));

        if (aHead == bHead) {
            return aHead.value;
        }
        Node<T> bigHead = leaderSizeMap.get(aHead) > leaderSizeMap.get(bHead) ? aHead : bHead;
        Node<T> smallHead = bigHead == aHead ? bHead : aHead;

        //记录Id改变信息
        personIdChangeMap.put(smallHead.value.getPersonId(), bigHead.value.getPersonId());

        parentMap.put(smallHead, bigHead);
        leaderSizeMap.put(bigHead, leaderSizeMap.get(bigHead) + leaderSizeMap.get(smallHead));
        leaderSizeMap.remove(smallHead);
        return bigHead.value;
    }

    public void setPersonIdChangeMap(Map<String, String> personIdChangeMap) {
        this.personIdChangeMap = personIdChangeMap;
    }

    public Map<String, String> getPersonIdChangeMap() {
        return personIdChangeMap;
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

    private static class Node<V> implements Serializable {
        V value;

        public Node(V val) {
            value = val;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Node<?> node = (Node<?>) o;
            return Objects.equals(value, node.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }
}
