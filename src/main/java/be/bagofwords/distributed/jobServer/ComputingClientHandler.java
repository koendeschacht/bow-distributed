package be.bagofwords.distributed.jobServer;

import be.bagofwords.distributed.shared.ProtocolConstants;
import be.bagofwords.distributed.shared.RemoteApplicationContextFactory;
import be.bagofwords.distributed.shared.RemoteJob;
import be.bagofwords.ui.UI;
import be.bagofwords.util.*;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class ComputingClientHandler extends SafeThread {

    private static int idCounter = 0;

    private final WrappedSocketConnection socketConnection;
    private final RemoteJobService remoteJobService;
    private RemoteApplicationContextFactory applicationContextFactory;
    private final int id;

    private RemoteJob currentJob;
    private long startOfJob;
    private long totalTimeOfExecutedJobs;
    private long totalTimeSendingJobs;
    private long creationTime;
    private int numberOfJobs;

    public ComputingClientHandler(WrappedSocketConnection socketConnection, RemoteJobService remoteJobService, RemoteApplicationContextFactory applicationContextFactory) {
        super("ComputingClientHandler", false);
        this.socketConnection = socketConnection;
        this.remoteJobService = remoteJobService;
        this.applicationContextFactory = applicationContextFactory;
        startOfJob = -1;
        synchronized (ComputingClientAccepter.class) {
            id = idCounter++;
        }
        this.totalTimeOfExecutedJobs = 0;
        this.creationTime = System.currentTimeMillis();
        this.numberOfJobs = 0;
    }

    public int getHandlerId() {
        return id;
    }

    public void runInt() {
        try {
            UI.write("### Computing client handler started for client " + getHost() + " " + getHandlerId());
            remoteJobService.registerComputingClient(this);
            int lastMessageRead;
            while (!isTerminateRequested() && (lastMessageRead = socketConnection.readByte()) != -1) {
                if (lastMessageRead == ProtocolConstants.REQUEST_JOB) {
                    currentJob = remoteJobService.selectJobForExecution();
                    while (!isTerminateRequested() && currentJob == null) {
                        Utils.threadSleep(200);
                        currentJob = remoteJobService.selectJobForExecution();
                    }
                    if (currentJob != null) {
                        //execute job
                        socketConnection.writeByte(ProtocolConstants.SENDING_JOB_TO_COMPUTING_CLIENT);
                        socketConnection.writeString(currentJob.getClass().getName());
                        socketConnection.writeByteArray(SerializationUtils.objectToCompressedBytes(currentJob, (Class) currentJob.getClass()));
                        socketConnection.flush();
                        startOfJob = System.currentTimeMillis();
                    }
                } else if (lastMessageRead == ProtocolConstants.SENDING_JOB_BACK_TO_SERVER) {
                    long start = System.currentTimeMillis();
                    String className = socketConnection.readString();
                    RemoteJob job = (RemoteJob) SerializationUtils.compressedBytesToObject(socketConnection.readByteArray(), Class.forName(className));
                    if (job.getId() != currentJob.getId()) {
                        throw new RuntimeException("Received an incorrect job!");
                    }
                    long end = System.currentTimeMillis();
                    job.addTimeUsedForSendingJob(end - start);
                    jobExecuted(job);
                    remoteJobService.jobFinished(job);
                    currentJob = null;
                    startOfJob = -1;
                } else if (lastMessageRead == ProtocolConstants.ASKING_OBJECT_FROM_SERVER) {
                    String className = socketConnection.readString();
                    byte[] classData = remoteJobService.getClassData(className);
                    sendClassData(className, classData);
                } else if (lastMessageRead == ProtocolConstants.JOB_IS_TERMINATED) {
                    UI.write("<-- Job " + currentJob.getId() + " is terminated");
                    startOfJob = -1;
                    currentJob = null;
                } else if (lastMessageRead == ProtocolConstants.READ_CLASS_DATAS) {
                    String locationPattern = socketConnection.readString();
                    List<Pair<String, byte[]>> classDatas = remoteJobService.getClassDatas(locationPattern);
                    socketConnection.writeByte(ProtocolConstants.SENDING_CLASS_DATAS_TO_CLIENT);
                    socketConnection.writeInt(classDatas.size());
                    for (Pair<String, byte[]> classData : classDatas) {
                        socketConnection.writeString(classData.getFirst());
                        socketConnection.writeByteArray(classData.getSecond());
                    }
                    socketConnection.flush();
                } else if (lastMessageRead == ProtocolConstants.READ_CONTEXT_FACTORY_CLASS) {
                    socketConnection.writeByte(ProtocolConstants.SEND_CONTEXT_FACTORY_CLASS);
                    if (applicationContextFactory == null) {
                        socketConnection.writeString("");
                    } else {
                        socketConnection.writeString(applicationContextFactory.getClass().getName());
                    }
                    socketConnection.flush();
                } else {
                    throw new RuntimeException("Received unexpected message " + lastMessageRead);
                }
            }
            Utils.threadSleep(200);
            if (!isTerminateRequested()) {
                UI.write("The connection with " + getHost() + " " + getHandlerId() + " died unexpectedly...");
            }
        } catch (Throwable e) {
            if (!isTerminateRequested()) {
                UI.writeError("Received unexpected exception for " + getHandlerId(), e);
            }
        }
        if (!isTerminateRequested()) {
            closeConnection();
        }

        UI.write("### Computing client handler finished for " + getHost() + " " + getHandlerId());
        if (currentJob != null) {
            remoteJobService.runRemoteJob(currentJob); //place job back in queue
        }
        remoteJobService.deregisterComputingClient(this);
    }

    private void jobExecuted(RemoteJob job) {
        totalTimeOfExecutedJobs += job.getTimeUsedForRunningJob();
        totalTimeSendingJobs += job.getTotalTimeUsedForSendingJob();
        numberOfJobs++;
    }

    public void sendClassData(String name, byte[] data) {
        try {
            synchronized (socketConnection) {
//                UI.write("Sending class data " + name + " to " + getHost());
                socketConnection.writeByte(ProtocolConstants.SENDING_OBJECT_TO_COMPUTING_CLIENT);
                socketConnection.writeString(name);
                if (data != null) {
                    socketConnection.writeBoolean(true);
                    socketConnection.writeByteArray(data);
                } else {
                    socketConnection.writeBoolean(false);
                }
                socketConnection.flush();
            }
        } catch (IOException exp) {
            throw new RuntimeException("Unexpected exception while trying to read class data", exp);
        }
    }

    public long getTimeTaken() {
        if (startOfJob == -1) {
            return 0;
        } else {
            return System.currentTimeMillis() - startOfJob;
        }
    }

    public void closeConnection() {
        terminate();
        IOUtils.closeQuietly(socketConnection);
    }

    public String getHost() {
        if (socketConnection != null)
            return ((InetSocketAddress) socketConnection.getSocket().getRemoteSocketAddress()).getHostName();
        else
            return "UNKNOWN";
    }

    public double getAverageTimePerJob() {
        if (getNumberOfJobs() > 0)
            return getTotalTimeOfExecutedJobs() / (double) getNumberOfJobs();
        else
            return 0;
    }

    public int getNumberOfJobs() {
        return numberOfJobs;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getTotalTimeOfExecutedJobs() {
        return totalTimeOfExecutedJobs;
    }

    public RemoteJob getCurrentJob() {
        return currentJob;
    }

    public double getAverageSendTime() {
        return totalTimeSendingJobs / (double) getNumberOfJobs();
    }

    @Override
    protected void doTerminate() {
        IOUtils.closeQuietly(socketConnection);
    }
}
