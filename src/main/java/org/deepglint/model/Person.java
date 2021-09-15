package org.deepglint.model;

import java.util.Objects;

/**
 * 处理后的数据
 *
 * @author ZhangFuQi
 * @date 2021/9/6 16:41
 */
public class Person extends RawData {
    private String personId;

    public Person() {
    }

    public Person(String personId) {
        this.personId = personId;
    }

    public Person(String objectId, String feature, String imgUrl, String personId) {
        super(objectId, feature, imgUrl);
        this.personId = personId;
    }

    public String getPersonId() {
        return personId;
    }

    public void setPersonId(String personId) {
        this.personId = personId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        Person person = (Person) o;
        return Objects.equals(personId, person.personId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), personId);
    }

    @Override
    public String toString() {
        return "Person{" +
                "personId='" + personId + '\'' + "," +
                "objectId='" + getObjectId() + '\'' + "," +
                "imgUrl='" + getImgUrl() + '\'' +
                "} ";
    }
}
