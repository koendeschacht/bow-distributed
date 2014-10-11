package be.bagofwords.distributed.computingClient;

import be.bagofwords.application.BaseRunnableApplicationContextFactory;
import be.bagofwords.application.MainClass;
import be.bagofwords.distributed.shared.RemoteApplicationContextFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/5/14.
 */
public class ComputingClientApplicationContextFactory extends BaseRunnableApplicationContextFactory {

    private RemoteApplicationContextFactory clientApplicationContextFactory;
    private ComputingClientConnection connection;

    public ComputingClientApplicationContextFactory(MainClass mainClass, RemoteApplicationContextFactory clientApplicationContextFactory, ComputingClientConnection connection) {
        super(mainClass);
        this.clientApplicationContextFactory = clientApplicationContextFactory;
        this.connection = connection;
    }

    @Override
    public AnnotationConfigApplicationContext createApplicationContext() {
        AnnotationConfigApplicationContext context = clientApplicationContextFactory.createApplicationContext();
        context.setResourceLoader(new NetworkedResourceLoader(connection));
        context.setClassLoader(connection.getNetworkedClassLoader());
        return context;
    }
}
