package be.bagofwords.distributed.shared;

import be.bagofwords.application.annotations.BowObject;
import be.bagofwords.application.status.perf.Trace;
import be.bagofwords.counts.Counter;

@BowObject
public abstract class RemoteJob {

    private static int idCounter;

    private String jobName;
    private int id;

    private long totalTimeUsedForSendingJob;
    private long timeUsedForRunningJob;
    private String computingClientHostName;
    private String exception;
    private Counter<Trace> relevantTracesCounter;
    private Counter<Trace> lessRelevantTracesCounter;


    public RemoteJob(String jobName) {
        synchronized (RemoteJob.class) {
            this.id = idCounter++;
        }
        this.jobName = jobName;
    }

    public abstract void execute() throws Exception;

    public String getJobName() {
        return jobName;
    }

    public int getId() {
        return id;
    }

    public long getTotalTimeUsedForSendingJob() {
        return totalTimeUsedForSendingJob;
    }

    public void setTotalTimeUsedForSendingJob(long totalTimeUsedForSendingJob) {
        this.totalTimeUsedForSendingJob = totalTimeUsedForSendingJob;
    }

    public long getTimeUsedForRunningJob() {
        return timeUsedForRunningJob;
    }

    public void setTimeUsedForRunningJob(long timeUsedForRunningJob) {
        this.timeUsedForRunningJob = timeUsedForRunningJob;
    }

    public String getComputingClientHostName() {
        return computingClientHostName;
    }

    public void setComputingClientHostName(String computingClientHostName) {
        this.computingClientHostName = computingClientHostName;
    }

    public String getException() {
        return exception;
    }

    public void setException(String exceptionStackTrace) {
        this.exception = exceptionStackTrace;
    }

    //Serialization

    protected RemoteJob() {
        synchronized (RemoteJob.class) {
            this.id = idCounter++;
        }
    }

    public void addTimeUsedForSendingJob(long time) {
        totalTimeUsedForSendingJob += time;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Counter<Trace> getRelevantTracesCounter() {
        return relevantTracesCounter;
    }

    public void setRelevantTracesCounter(Counter<Trace> relevantTracesCounter) {
        this.relevantTracesCounter = relevantTracesCounter;
    }

    public Counter<Trace> getLessRelevantTracesCounter() {
        return lessRelevantTracesCounter;
    }

    public void setLessRelevantTracesCounter(Counter<Trace> lessRelevantTracesCounter) {
        this.lessRelevantTracesCounter = lessRelevantTracesCounter;
    }
}
