package be.bagofwords.main;

import be.bagofwords.application.ApplicationManager;
import be.bagofwords.application.status.perf.ThreadSampleMonitor;
import be.bagofwords.distributed.computingClient.ComputingClient;
import be.bagofwords.distributed.computingClient.ComputingClientApplicationContextFactory;
import be.bagofwords.distributed.computingClient.ComputingClientConnection;
import be.bagofwords.distributed.shared.RemoteApplicationContextFactory;
import be.bagofwords.ui.UI;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class ComputingClientMain {

    public static void main(String[] args) {
        String remoteJobServerAddress = JobServerMapping.getJobServerAddress();
        ComputingClientConnection connection = null;
        String applicationContextFactoryClassName = null;
        try {
            connection = new ComputingClientConnection(remoteJobServerAddress);
            applicationContextFactoryClassName = connection.readContextFactoryClassName();
            Thread.currentThread().setContextClassLoader(connection.getNetworkedClassLoader());
        } catch (IOException exp) {
            UI.write("Failed to connect to " + remoteJobServerAddress);
        }
        if (connection != null) {
            if (StringUtils.isEmpty(applicationContextFactoryClassName)) {
                //run application without spring context
                ComputingClient computingClient = new ComputingClient(connection);
                computingClient.setThreadSampleMonitor(new ThreadSampleMonitor(false, null, null));
                computingClient.run();
            } else {
                //run application with spring context
                RemoteApplicationContextFactory clientFactory = createApplicationConfigurationInstance(connection, applicationContextFactoryClassName);
                ComputingClientApplicationContextFactory contextFactory = new ComputingClientApplicationContextFactory(new ComputingClient(connection), clientFactory, connection);
                ApplicationManager.runSafely(contextFactory);
            }
        }
    }

    private static RemoteApplicationContextFactory createApplicationConfigurationInstance(ComputingClientConnection connection, String applicationContextFactoryClassName) {
        try {
            Class<? extends RemoteApplicationContextFactory> applicationConfigurationClass = connection.getNetworkedClassLoader().loadClass(applicationContextFactoryClassName);
            return applicationConfigurationClass.getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Unexpected error while creating environment properties factory instance.", e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Factory class " + applicationContextFactoryClassName + " should have a constructor with zero arguments", e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Factory class " + applicationContextFactoryClassName + " could not be loaded.", e);
        }
    }


}
