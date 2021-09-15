package org.deepglint;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.deepglint.arithmetic.UnionFindCluster;
import org.deepglint.model.Person;
import org.deepglint.model.RawData;
import org.deepglint.util.MyCsvOutputFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 启动类
 *
 * @author ZhangFuQi
 * @date 2021/9/6 16:50
 */
public class Main {
    public static final String FILE_PATH = "C:\\Users\\Administrator\\IdeaProjects\\cluster-demo\\src\\main\\resources\\input.csv";
    public static final String OUTPUT_FILE_PATH = "C:\\Users\\Administrator\\IdeaProjects\\cluster-demo\\src\\main\\resources\\output.csv";

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        DataSet<RawData> rawData = readCsv(env);
        UnionFindCluster<RawData> cluster = new UnionFindCluster<>(5000);
        List<Cluster<RawData>> clusterList = cluster.cluster(rawData.collect());
        List<Person> personList = new ArrayList<>();
        for (Cluster<RawData> rawDataCluster : clusterList) {
            List<RawData> points = rawDataCluster.getPoints();
            String pId = UUID.randomUUID().toString();
            for (RawData point : points) {
                Person person = new Person();
                person.setPersonId(pId);
                person.setImgUrl(point.getImgUrl());
                person.setObjectId(point.getObjectId());
                personList.add(person);
            }
        }

        env.fromCollection(personList).map(new MapFunction<Person, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(Person value) throws Exception {
                return new Tuple3<>(value.getObjectId(), value.getPersonId(), value.getImgUrl());
            }
        }).output(new MyCsvOutputFormat<>(new Path(OUTPUT_FILE_PATH)));
        env.execute();
    }

    private static void dbScan(ExecutionEnvironment env, DataSet<RawData> rawData) throws Exception {
        DBSCANClusterer<RawData> cluster = new DBSCANClusterer<>(0.8, 12);
        List<Cluster<RawData>> clusterList = cluster.cluster(rawData.collect());
        List<Person> personList = new ArrayList<>();
        for (Cluster<RawData> rawDataCluster : clusterList) {
            List<RawData> points = rawDataCluster.getPoints();
            String pId = UUID.randomUUID().toString();
            for (RawData point : points) {
                Person person = new Person();
                person.setPersonId(pId);
                person.setImgUrl(point.getImgUrl());
                person.setObjectId(point.getObjectId());
                personList.add(person);
            }
        }

        env.fromCollection(personList).map(new MapFunction<Person, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(Person value) throws Exception {
                return new Tuple3<>(value.getObjectId(), value.getPersonId(), value.getImgUrl());
            }
        }).output(new MyCsvOutputFormat<>(new Path(OUTPUT_FILE_PATH)));
        env.execute();
    }

    private static DataSet<RawData> readCsv(ExecutionEnvironment env) {
        return env.readCsvFile(FILE_PATH)
                .fieldDelimiter(",")
                .ignoreFirstLine()
                .includeFields(true, true, true)
                .pojoType(RawData.class, "objectId", "feature", "imgUrl");
    }
}
