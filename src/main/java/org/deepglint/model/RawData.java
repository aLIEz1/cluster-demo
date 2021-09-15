package org.deepglint.model;

import org.apache.commons.math3.ml.clustering.Clusterable;
import org.deepglint.util.FeatureDecoder;

import java.util.List;
import java.util.Objects;

/**
 * 原始数据
 *
 * @author ZhangFuQi
 * @date 2021/9/6 16:37
 */
public class RawData implements Clusterable {
    private String imgUrl;
    private String objectId;
    private String feature;

    public RawData() {
    }

    public RawData(String objectId, String feature, String imgUrl) {
        this.objectId = objectId;
        this.feature = feature;
        this.imgUrl = imgUrl;
    }

    public String getObjectId() {
        return objectId;
    }

    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }

    public String getFeature() {
        return feature;
    }

    public void setFeature(String feature) {
        this.feature = feature;
    }

    public String getImgUrl() {
        return imgUrl;
    }

    public void setImgUrl(String imgUrl) {
        this.imgUrl = imgUrl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RawData rawData = (RawData) o;
        return objectId.equals(rawData.objectId) && feature.equals(rawData.feature) && imgUrl.equals(rawData.imgUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, feature, imgUrl);
    }

    @Override
    public String toString() {
        return "RawData{" +
                "objectId='" + objectId + '\'' +
                ", feature='" + feature + '\'' +
                ", imgUrl='" + imgUrl + '\'' +
                '}';
    }

    public float cosineSimilarity(RawData other) {
        List<Float> a = FeatureDecoder.decode(this.getFeature());
        List<Float> b = FeatureDecoder.decode(other.getFeature());
        int size = a.size();
        float ans = 0;
        for (int i = 0; i < size; i++) {
            ans += a.get(i) * b.get(i);
        }
        return ans;
    }

    @Override
    public double[] getPoint() {
        return FeatureDecoder.decode(this.getFeature()).stream().mapToDouble(i -> i).toArray();
    }
}
