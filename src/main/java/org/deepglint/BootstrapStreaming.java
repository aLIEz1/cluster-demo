package org.deepglint;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.deepglint.arithmetic.UnionFindProcessFunction;
import org.deepglint.arithmetic.UnionSet;
import org.deepglint.model.Person;
import org.deepglint.model.PersonChangeInfo;
import org.deepglint.model.RawData;
import org.deepglint.model.Tags;
import org.deepglint.sink.MyJdbcSink;

/**
 * @author ZhangFuQi
 * @date 2021/9/8 15:30
 */
public class BootstrapStreaming {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setParallelism(1);

        Path path = new Path(Main.FILE_PATH);
        PojoTypeInfo<RawData> sourceType = (PojoTypeInfo<RawData>) TypeExtractor.getForClass(RawData.class);

        CsvInputFormat<RawData> format = new PojoCsvInputFormat<>(path,
                CsvInputFormat.DEFAULT_LINE_DELIMITER,
                CsvInputFormat.DEFAULT_FIELD_DELIMITER,
                sourceType,
                new String[]{"objectId", "feature", "imgUrl"}
        );

        UnionSet<Person> personUnionSet = new UnionSet<>();
        SingleOutputStreamOperator<Person> operator = env.createInput(format, sourceType)
                .setParallelism(1)
                .filter((FilterFunction<RawData>) value -> !"obj_id".equals(value.getObjectId()))
                .process(new UnionFindProcessFunction(personUnionSet));

        SingleOutputStreamOperator<Tuple4<String, String, String, String>> process = operator
                .map((MapFunction<Person, Tuple4<String, String, String, String>>) value -> new Tuple4<>(value.getObjectId(), value.getPersonId(), value.getImgUrl(), value.getFeature()))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING));


        process.writeAsCsv(Main.OUTPUT_FILE_PATH, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataStream<PersonChangeInfo> sideOutput1 = operator.getSideOutput(Tags.CHANGE_LIST_TAG);
        DataStreamSink<PersonChangeInfo> sideOutput = sideOutput1
                .addSink(new MyJdbcSink());
        //        sideOutput1.print();
        env.execute();
    }
}