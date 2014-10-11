package be.bagofwords.distributed.test;

import be.bagofwords.application.BaseApplicationContextFactory;
import be.bagofwords.distributed.shared.RemoteApplicationContextFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/5/14.
 */
public class TestRemoteJobServiceApplicationContextFactory extends BaseApplicationContextFactory implements RemoteApplicationContextFactory {

    @Override
    public AnnotationConfigApplicationContext createApplicationContext() {
        singleton("environmentProperties", new TestEnvironmentProperties());
        scan("be.bagofwords");
        return super.createApplicationContext();
    }
}
