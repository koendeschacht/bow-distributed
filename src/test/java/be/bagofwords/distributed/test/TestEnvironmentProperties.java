package be.bagofwords.distributed.test;

import be.bagofwords.application.EnvironmentProperties;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/5/14.
 */
public class TestEnvironmentProperties implements EnvironmentProperties {
    @Override
    public boolean saveThreadSamplesToFile() {
        return false;
    }

    @Override
    public String getThreadSampleLocation() {
        return null;
    }

    @Override
    public String getApplicationUrlRoot() {
        return "localhost";
    }
}
