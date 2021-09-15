package org.deepglint.model;

import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * @author ZhangFuQi
 * @date 2021/9/13 11:29
 */
public interface Tags {
    OutputTag<PersonChangeInfo> CHANGE_LIST_TAG = new OutputTag<PersonChangeInfo>("changeList") {
    };
}
