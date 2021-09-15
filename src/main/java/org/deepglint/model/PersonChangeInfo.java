package org.deepglint.model;

import java.util.Objects;

/**
 * @author ZhangFuQi
 * @date 2021/9/15 10:19
 */
public class PersonChangeInfo {
    private String source;
    private String target;

    public PersonChangeInfo(String source, String target) {
        this.source = source;
        this.target = target;
    }

    public PersonChangeInfo() {
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PersonChangeInfo that = (PersonChangeInfo) o;
        return Objects.equals(source, that.source) && Objects.equals(target, that.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, target);
    }

    @Override
    public String toString() {
        return "PersonChangeInfo{" +
                "source='" + source + '\'' +
                ", target='" + target + '\'' +
                '}';
    }
}
