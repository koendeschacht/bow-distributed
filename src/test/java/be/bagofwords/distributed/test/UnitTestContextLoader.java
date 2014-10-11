package be.bagofwords.distributed.test;

import be.bagofwords.application.ApplicationContextFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.context.ContextLoader;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/25/14.
 */
public class UnitTestContextLoader implements ContextLoader {
    @Override
    public String[] processLocations(Class<?> aClass, String... locations) {
        return locations;
    }

    @Override
    public ApplicationContext loadContext(String... strings) throws Exception {
        ApplicationContextFactory applicationContextFactory = new TestRemoteJobServiceApplicationContextFactory();
        AnnotationConfigApplicationContext applicationContext = applicationContextFactory.createApplicationContext();
        return applicationContext;
    }
}
