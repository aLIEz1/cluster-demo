package org.deepglint.util;

import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.deepglint.Main;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * csv输出
 *
 * @author ZhangFuQi
 * @date 2021/9/7 16:07
 */
public class MyCsvOutputFormat<T extends Tuple> extends CsvOutputFormat<T> {
    public MyCsvOutputFormat(Path outputPath) {
        super(outputPath);
        super.setWriteMode(FileSystem.WriteMode.OVERWRITE);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try (PrintWriter writer = new PrintWriter(Main.OUTPUT_FILE_PATH)) {
            writer.println("obj_id,person_id,img_url");
        }
        super.open(taskNumber, numTasks);
    }
}
