package be.bagofwords.distributed.jobServer;

import be.bagofwords.distributed.shared.Constants;
import be.bagofwords.distributed.shared.RemoteApplicationContextFactory;
import be.bagofwords.util.SafeThread;
import be.bagofwords.util.WrappedSocketConnection;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ComputingClientAccepter extends SafeThread {

    private RemoteJobService server;
    private RemoteApplicationContextFactory applicationContextFactory;
    private ServerSocket serverSocket;

    public ComputingClientAccepter(RemoteJobService server, RemoteApplicationContextFactory applicationContextFactory) {
        super("ComputingClientAccepter", false);
        this.server = server;
        this.applicationContextFactory = applicationContextFactory;
    }

    public void runInt() {
        try {
            serverSocket = new ServerSocket(Constants.SERVER_PORT_FOR_COMPUTING_CLIENTS);
            while (!isTerminateRequested()) {
                Socket clientSocket = serverSocket.accept();
                WrappedSocketConnection wrappedSocketConnection = new WrappedSocketConnection(clientSocket);
                ComputingClientHandler handler = new ComputingClientHandler(wrappedSocketConnection, server, applicationContextFactory);
                handler.start();
            }
            IOUtils.closeQuietly(serverSocket);
        } catch (IOException exp) {
            if (!isTerminateRequested()) {
                throw new RuntimeException(exp);
            }
        }
    }

    @Override
    protected void doTerminate() {
        IOUtils.closeQuietly(serverSocket);
    }
}
