package org.deepglint.arithmetic;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.deepglint.model.Person;
import org.deepglint.model.RawData;
import org.deepglint.util.IdUtils;

/**
 * 处理类
 *
 * @author ZhangFuQi
 * @date 2021/9/8 17:15
 */
public class UnionFindProcessFunction extends ProcessFunction<RawData, Person> {
    private UnionSet<Person> personUnionSet;

    public UnionFindProcessFunction() {
    }

    public UnionFindProcessFunction(UnionSet<Person> personUnionSet) {
        this.personUnionSet = personUnionSet;
    }

    @Override
    public void processElement(RawData value,
                               Context ctx,
                               Collector<Person> out) throws Exception {
        Person person = new Person();
        String randomId = IdUtils.getRandomId();
        person.setPersonId(randomId);
        person.setFeature(value.getFeature());
        person.setObjectId(value.getObjectId());
        person.setImgUrl(value.getImgUrl());
        String s = personUnionSet.addElement(person, ctx);
        Person person1 = new Person();
        person1.setPersonId(s);
        person1.setImgUrl(value.getImgUrl());
        person1.setObjectId(value.getObjectId());
        person1.setFeature(value.getFeature());
        out.collect(person1);
    }
}
