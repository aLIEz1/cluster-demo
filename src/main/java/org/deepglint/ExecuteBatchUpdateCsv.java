package org.deepglint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.deepglint.model.Person;
import org.deepglint.util.DbUtil;
import org.deepglint.util.MyCsvOutputFormat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 批量更新csv文件,后聚类处理脚本
 *
 * @author ZhangFuQi
 * @date 2021/9/10 11:34
 */
public class ExecuteBatchUpdateCsv {

    public static void execute() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Person> personDataSet = readCsv(env);
        List<Person> personList = personDataSet.collect();
        ArrayList<HashMap<String, String>> query = DbUtil.executeQuery("select change_from,change_to from change_list");
        for (Person person : personList) {
            for (HashMap<String, String> map : query) {
                if (person.getPersonId().equals(map.get("change_from"))) {
                    person.setPersonId(map.get("change_to"));
                }
            }
        }
        env.fromCollection(personList).map(new MapFunction<Person, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(Person value) throws Exception {
                return new Tuple3<>(value.getObjectId(), value.getPersonId(), value.getImgUrl());
            }
        }).output(new MyCsvOutputFormat<>(new Path(Main.OUTPUT_FILE_PATH)));
        env.execute();
    }

    private static DataSet<Person> readCsv(ExecutionEnvironment env) {
        return env.readCsvFile(Main.OUTPUT_FILE_PATH)
                .fieldDelimiter(",")
                .includeFields(true, true, true, true)
                .pojoType(Person.class, "objectId", "personId", "imgUrl", "feature");
    }

    public static void main(String[] args) throws Exception {
        ExecuteBatchUpdateCsv.execute();
    }

}
