package be.bagofwords.distributed.computingClient;


import be.bagofwords.distributed.shared.Constants;
import be.bagofwords.distributed.shared.ProtocolConstants;
import be.bagofwords.distributed.shared.RemoteJob;
import be.bagofwords.ui.UI;
import be.bagofwords.util.Pair;
import be.bagofwords.util.SerializationUtils;
import be.bagofwords.util.WrappedSocketConnection;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class ComputingClientConnection implements Closeable {

    private WrappedSocketConnection socketConnection;
    private String localHostName;
    private NetworkedClassLoader networkedClassLoader = null;

    public ComputingClientConnection(String jobServerAddress) throws IOException {
        socketConnection = new WrappedSocketConnection(jobServerAddress, Constants.SERVER_PORT_FOR_COMPUTING_CLIENTS);
        networkedClassLoader = new NetworkedClassLoader(ClassLoader.getSystemClassLoader(), this);
        try {
            InetAddress addr = InetAddress.getLocalHost();
            this.localHostName = addr.getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        UI.write("Successfully connected to job server " + jobServerAddress);
    }

    @Override
    public synchronized void close() throws IOException {
        socketConnection.close();
    }

    public synchronized RemoteJob readJob() throws IOException {
        socketConnection.writeByte(ProtocolConstants.REQUEST_JOB);
        socketConnection.flush();
        byte message = socketConnection.readByte();
        if (message != ProtocolConstants.SENDING_JOB_TO_COMPUTING_CLIENT) {
            throw new RuntimeException("Unexpected message " + message);
        }
        long start = System.currentTimeMillis();
        final String className = socketConnection.readString();
        byte[] compressedJob = socketConnection.readByteArray();
        long end = System.currentTimeMillis();
        RemoteJob job = (RemoteJob) SerializationUtils.compressedBytesToObject(compressedJob, findClass(className));
        job.addTimeUsedForSendingJob(end - start);
        job.setComputingClientHostName(localHostName);
        return job;
    }

    public Class<Object> findClass(String className) {
        try {
            return networkedClassLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized byte[] readClassData(String name) throws IOException {
        socketConnection.writeByte(ProtocolConstants.ASKING_OBJECT_FROM_SERVER);
        socketConnection.writeString(name);
        socketConnection.flush();
        byte lastMessageRead = socketConnection.readByte();
        if (lastMessageRead == ProtocolConstants.SENDING_OBJECT_TO_COMPUTING_CLIENT) {
            String sendId = socketConnection.readString();
            boolean success = socketConnection.readBoolean();
            if (sendId.equals(name)) {
                if (success) {
                    return socketConnection.readByteArray();
                } else {
                    return null;
                }
            } else {
                throw new RuntimeException("Received data for object " + sendId + " while we were waiting for data for " + name);
            }
        } else {
            throw new RuntimeException("### Received unexpected message " + lastMessageRead);
        }
    }

    public synchronized void flush() throws IOException {
        socketConnection.flush();
    }

    public synchronized void sendJobBack(RemoteJob job) throws IOException {
        socketConnection.writeByte(ProtocolConstants.SENDING_JOB_BACK_TO_SERVER);
        socketConnection.writeString(job.getClass().getName());
        socketConnection.writeByteArray(SerializationUtils.objectToCompressedBytes(job, (Class) job.getClass()));
    }

    public NetworkedClassLoader getNetworkedClassLoader() {
        return networkedClassLoader;
    }

    public synchronized List<Pair<String, byte[]>> readClassDatas(String locationPattern) throws IOException {
        List<Pair<String, byte[]>> result = new ArrayList<>();
        socketConnection.writeByte(ProtocolConstants.READ_CLASS_DATAS);
        socketConnection.writeString(locationPattern);
        socketConnection.flush();
        byte message = socketConnection.readByte();
        if (message == ProtocolConstants.SENDING_CLASS_DATAS_TO_CLIENT) {
            int numOfDatas = socketConnection.readInt();
            for (int i = 0; i < numOfDatas; i++) {
                String name = socketConnection.readString();
                byte[] data = socketConnection.readByteArray();
                result.add(new Pair<>(name, data));
            }
            return result;
        } else {
            throw new RuntimeException("Received unexpected message " + message);
        }
    }

    public synchronized String readContextFactoryClassName() throws IOException {
        socketConnection.writeByte(ProtocolConstants.READ_CONTEXT_FACTORY_CLASS);
        socketConnection.flush();
        byte message = socketConnection.readByte();
        if (message != ProtocolConstants.SEND_CONTEXT_FACTORY_CLASS) {
            throw new RuntimeException("Received unexpected message " + message);
        }
        return socketConnection.readString();
    }
}
