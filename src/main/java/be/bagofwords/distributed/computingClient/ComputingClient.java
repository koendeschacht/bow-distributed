package be.bagofwords.distributed.computingClient;

import be.bagofwords.application.MainClass;
import be.bagofwords.application.status.perf.ThreadSampleMonitor;
import be.bagofwords.application.status.perf.Trace;
import be.bagofwords.counts.Counter;
import be.bagofwords.distributed.shared.RemoteJob;
import be.bagofwords.ui.UI;
import be.bagofwords.util.SafeThread;
import be.bagofwords.util.Utils;
import org.springframework.context.ApplicationContext;

public class ComputingClient extends SafeThread implements MainClass {

    private ApplicationContext applicationContext;  //is null when used without spring context
    private ThreadSampleMonitor threadSampleMonitor;
    private final ComputingClientConnection connection;

    public ComputingClient(ComputingClientConnection connection, ThreadSampleMonitor threadSampleMonitor) {
        super("ComputingClient", false);
        this.connection = connection;
        this.threadSampleMonitor = threadSampleMonitor;
    }

    public ComputingClient(ComputingClientConnection connection, ThreadSampleMonitor threadSampleMonitor, ApplicationContext applicationContext) {
        super("ComputingClient", false);
        this.connection = connection;
        this.threadSampleMonitor = threadSampleMonitor;
        this.applicationContext = applicationContext;
    }

    public void runInt() {
        try {
            while (!isTerminateRequested()) {
                RemoteJob job = connection.readJob();
                long startOfExecution = System.currentTimeMillis();
                threadSampleMonitor.clearSamples();
                try {
                    if (applicationContext != null) {
                        autoWire(job);
                    }
                    job.execute();
                } catch (Throwable t) {
                    UI.writeError("Caught exception while running job", t);
                    job.setException(Utils.getStackTrace(t));
                }
                job.setTimeUsedForRunningJob(System.currentTimeMillis() - startOfExecution);
                attachThreadSamples(job);
                connection.sendJobBack(job);
                UI.write("## Finished executing job " + job.getJobName() + " " + job.getId());
                connection.flush();
            }
        } catch (Throwable e) {
            UI.writeError("Unexpected error in computing client ", e);
        }
    }

    private void autoWire(RemoteJob job) {
        applicationContext.getAutowireCapableBeanFactory().autowireBean(job);
    }

    private void attachThreadSamples(RemoteJob job) {
        synchronized (threadSampleMonitor.getRelevantTracesCounter()) {
            Counter<Trace> clone = threadSampleMonitor.getRelevantTracesCounter().clone();
            clone.trim(ThreadSampleMonitor.MAX_NUM_OF_SAMPLES / 10);
            job.setRelevantTracesCounter(clone);
        }
        synchronized (threadSampleMonitor.getLessRelevantTracesCounter()) {
            Counter<Trace> clone = threadSampleMonitor.getLessRelevantTracesCounter().clone();
            clone.trim(ThreadSampleMonitor.MAX_NUM_OF_SAMPLES / 10);
            job.setLessRelevantTracesCounter(clone);
        }
        job.setNumOfSamples(threadSampleMonitor.getNumOfSamples());
    }

}
