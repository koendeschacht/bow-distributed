package be.bagofwords.distributed.jobServer;

import be.bagofwords.application.CloseableComponent;
import be.bagofwords.application.annotations.BowComponent;
import be.bagofwords.application.status.StatusViewable;
import be.bagofwords.application.status.perf.ThreadSampleMonitor;
import be.bagofwords.application.status.perf.ThreadSamplesPrinter;
import be.bagofwords.application.status.perf.Trace;
import be.bagofwords.counts.Counter;
import be.bagofwords.counts.WindowOfCounts;
import be.bagofwords.distributed.shared.RemoteApplicationContextFactory;
import be.bagofwords.distributed.shared.RemoteJob;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.DataIterable;
import be.bagofwords.ui.UI;
import be.bagofwords.util.NumUtils;
import be.bagofwords.util.OccasionalAction;
import be.bagofwords.util.Pair;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

@BowComponent
public class RemoteJobService implements CloseableComponent, StatusViewable {

    private final List<RemoteJob> waitingJobs;
    private final List<RemoteJob> finishedJobs;
    private final List<ComputingClientHandler> computingClients;
    private final List<CachedClassData> cachedClassDatas;
    private final String waitForJobsMonitor = new String("WAIT_FOR_JOBS");
    private final Counter<Trace> relevantTracesCounter;
    private final Counter<Trace> lessRelevantTracesCounter;
    private int numOfSamples;
    private final ResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();

    private ComputingClientAccepter computingClientAccepter;
    private boolean terminateRequested = false;

    /**
     * Constructor called by spring when creating this class as a bean
     */

    @Autowired
    public RemoteJobService(RemoteApplicationContextFactory applicationContextFactory) {
        this.finishedJobs = new LinkedList<>();
        this.waitingJobs = new LinkedList<>();
        this.computingClients = new ArrayList<>();
        this.cachedClassDatas = new ArrayList<>();
        this.relevantTracesCounter = new Counter<>();
        this.numOfSamples = 0;
        this.lessRelevantTracesCounter = new Counter<>();
        this.computingClientAccepter = new ComputingClientAccepter(this, applicationContextFactory);
        this.computingClientAccepter.start();
    }

    /**
     * Constructor to be used in a non-spring context
     */

    public RemoteJobService() {
        this(null);
    }

    public void runRemoteJob(RemoteJob job) {
        synchronized (waitingJobs) {
            waitingJobs.add(job);
        }
    }

    public synchronized void jobFinished(RemoteJob job) {
        synchronized (finishedJobs) {
            this.finishedJobs.add(job);
        }
        addTraceSamples(job);
        synchronized (waitForJobsMonitor) {
            waitForJobsMonitor.notify();
        }
    }

    private void addTraceSamples(RemoteJob job) {
        synchronized (relevantTracesCounter) {
            relevantTracesCounter.addAll(job.getRelevantTracesCounter());
            relevantTracesCounter.trim(ThreadSampleMonitor.MAX_NUM_OF_SAMPLES / 2);
        }
        synchronized (lessRelevantTracesCounter) {
            lessRelevantTracesCounter.addAll(job.getLessRelevantTracesCounter());
            lessRelevantTracesCounter.trim(ThreadSampleMonitor.MAX_NUM_OF_SAMPLES / 2);
        }
        numOfSamples += job.getNumOfSamples();
    }

    private RemoteJob checkForException(RemoteJob job) {
        if (job.getException() != null) {
            throw new RuntimeException("Exception while executing job remotely on host " + job.getComputingClientHostName() + "! " + job.getException());
        } else {
            return job;
        }
    }

    @Override
    public void terminate() {
        terminateRequested = true;
        this.computingClientAccepter.terminate();
        synchronized (computingClients) {
            for (ComputingClientHandler computingClient : computingClients) {
                computingClient.terminate();
            }
        }
    }

    @Override
    public void printHtmlStatus(StringBuilder result) {
        try {
            String spaces = "  ";
            result.append("<h1>Remote Job Server</h1>");
            result.append("<pre>");
            result.append("###" + spaces + "Number of jobs in wait queue " + waitingJobs.size() + "\n");
            result.append("###" + spaces + "Number of cached classes " + cachedClassDatas.size() + "\n");
            result.append("###" + spaces + computingClients.size() + " computing clients\n");
            if (!computingClients.isEmpty()) {
                result.append("###" + spaces + spaces + "hostname - id - conn. time - av. exec time - av. send time - jobs exec. - curr job\n");
                for (ComputingClientHandler handler : computingClients) {
                    RemoteJob currentJob = handler.getCurrentJob();
                    if (currentJob != null) {
                        result.append("###" + spaces + spaces + handler.getHost() + " - " + handler.getHandlerId() + " - " + getTimeString(System.currentTimeMillis() - handler.getCreationTime())
                                + " - " + NumUtils.fmt(handler.getAverageTimePerJob()) + " - " + NumUtils.fmt(handler.getAverageSendTime()) + " - " + handler.getNumberOfJobs() + " - " + currentJob.getJobName() + " for "
                                + handler.getTimeTaken() + "\n");
                    } else
                        result.append("###" + spaces + spaces + handler.getHost() + " - " + handler.getHandlerId() + " - " + getTimeString(System.currentTimeMillis() - handler.getCreationTime()) + " - "
                                + NumUtils.fmt(handler.getAverageTimePerJob()) + " - " + NumUtils.fmt(handler.getAverageSendTime()) + " - " + handler.getNumberOfJobs() + "\n");
                }
            }
            result.append("</pre>");


            result.append("Collected " + relevantTracesCounter.getTotal() + " samples.");
            result.append("<h2>Relevant traces</h2><pre>");
            ThreadSamplesPrinter.printTopTraces(result, relevantTracesCounter, numOfSamples);
            result.append("</pre>");
            result.append("<h2>Other traces</h2><pre>");
            ThreadSamplesPrinter.printTopTraces(result, lessRelevantTracesCounter, numOfSamples);
            result.append("</pre>");

        } catch (Throwable t) {
            UI.writeError("error while printing status", t);
        }
    }

    public static String getTimeString(long milli) {
        long sec = milli / 1000;
        milli %= 1000;
        long min = sec / 60;
        sec %= 60;
        long hrs = min / 60;
        min %= 60;
        String secStr = sec < 10 ? "0" + sec : "" + sec;
        return hrs + ":" + min + ":" + secStr;
    }

    public <T> void runRemoteJobs(final String name, DataIterable<T> dataIterable, final RemoteJobFactory<T> remoteJobFactory) {
        final long apprSize = dataIterable.apprSize();
        final WindowOfCounts windowedCounts = new WindowOfCounts(60000);
        OccasionalAction<String> action = new OccasionalAction<String>(10000) {
            @Override
            protected void doAction(String curr) {
                long todo = apprSize - windowedCounts.getTotalCounts();
                UI.write("[Progress " + name + "] did " + windowedCounts.getTotalCounts() + " of " + apprSize + " end is " + new Date(System.currentTimeMillis() + windowedCounts.getNeededTime(todo)) + " curr is " + curr);
            }
        };
        CloseableIterator<T> iterator = dataIterable.iterator();
        ArrayList<Integer> jobsCurrentlySubmitted = new ArrayList<>();
        while (iterator.hasNext() && !terminateRequested) {
            waitForFinishedJobs(remoteJobFactory, windowedCounts, action, jobsCurrentlySubmitted, 200);
            while (iterator.hasNext() && jobsCurrentlySubmitted.size() <= 200) {
                T curr = iterator.next();
                RemoteJob remoteJob = remoteJobFactory.createRemoteJob(curr);
                if (remoteJob != null) {
                    runRemoteJob(remoteJob);
                    jobsCurrentlySubmitted.add(remoteJob.getId());
                }
            }
        }
        waitForFinishedJobs(remoteJobFactory, windowedCounts, action, jobsCurrentlySubmitted, 0);
        iterator.close();
    }

    private <T> void waitForFinishedJobs(RemoteJobFactory<T> remoteJobFactory, WindowOfCounts windowedCounts, OccasionalAction<String> action, ArrayList<Integer> jobsCurrentlySubmitted, int numOfRemainingJobs) {
        long timeSinceLastMessage = System.currentTimeMillis();
        while (jobsCurrentlySubmitted.size() > numOfRemainingJobs && !terminateRequested) {
            List<RemoteJob> finishedJobs = removeAllFinishedJobs(jobsCurrentlySubmitted);
            for (RemoteJob finishedJob : finishedJobs) {
                timeSinceLastMessage = System.currentTimeMillis();
                remoteJobFactory.remoteJobFinished(finishedJob);
                windowedCounts.addCount();
                action.doOccasionalAction(finishedJob.getJobName());
            }
            try {
                synchronized (waitForJobsMonitor) {
                    waitForJobsMonitor.wait(100);
                    if (computingClients.isEmpty() && (System.currentTimeMillis() - timeSinceLastMessage) > 10000) {
                        UI.write("Waiting for computing clients to connect...");
                        timeSinceLastMessage = System.currentTimeMillis();
                    }
                }
            } catch (InterruptedException e) {
                //OK
            }
        }
    }

    private List<RemoteJob> removeAllFinishedJobs(ArrayList<Integer> jobIds) {
        synchronized (finishedJobs) {
            List<RemoteJob> result = new ArrayList<>();
            for (int i = 0; i < finishedJobs.size(); i++) {
                RemoteJob finishedJob = finishedJobs.get(i);
                int index = jobIds.indexOf(finishedJob.getId());
                if (index >= 0) {
                    checkForException(finishedJob);
                    finishedJobs.remove(i--);
                    jobIds.remove(index);
                    result.add(finishedJob);
                }
            }
            return result;
        }
    }

    public void registerComputingClient(ComputingClientHandler computingClientHandler) {
        synchronized (computingClients) {
            computingClients.add(computingClientHandler);
        }
    }


    public void deregisterComputingClient(ComputingClientHandler handler) {
        synchronized (computingClients) {
            this.computingClients.remove(handler);
        }
    }

    public RemoteJob selectJobForExecution() {
        synchronized (waitingJobs) {
            if (!waitingJobs.isEmpty()) {
                return waitingJobs.remove(0);
            } else {
                return null;
            }
        }
    }

    public byte[] getClassData(String name) {
        synchronized (this) {
            //Check whether class is available in cached classes
            byte[] classBytes = getCachedObject(name);
            if (classBytes == null) {
                classBytes = ClassReader.getClass(name);
                synchronized (this) {
                    cachedClassDatas.add(new CachedClassData(name, classBytes));
                }
            }
            return classBytes;
        }
    }

    private byte[] getCachedObject(String name) {
        synchronized (this) {
            for (CachedClassData cachedClassData : cachedClassDatas) {
                if (cachedClassData.getName().equals(name))
                    return cachedClassData.getData();
            }
        }
        return null;
    }

    public List<Pair<String, byte[]>> getClassDatas(String locationPattern) throws IOException {
        Resource[] resources = resourcePatternResolver.getResources(locationPattern);
        List<Pair<String, byte[]>> result = new ArrayList<>();
        for (Resource resource : resources) {
            if (resource.isReadable()) {
                String name = resource.getDescription();
                byte[] bytes = new byte[(int) resource.contentLength()];
                IOUtils.read(resource.getInputStream(), bytes);
                result.add(new Pair<>(name, bytes));
            }
        }
        return result;
    }

    public static class CachedClassData {

        private String name;
        private byte[] data;

        public CachedClassData(String name, byte[] data) {
            super();
            this.name = name;
            this.data = data;
        }

        public String getName() {
            return name;
        }

        public byte[] getData() {
            return data;
        }

    }

}
