package be.bagofwords.distributed;


import be.bagofwords.distributed.jobServer.RemoteJobFactory;
import be.bagofwords.distributed.jobServer.RemoteJobService;
import be.bagofwords.distributed.shared.RemoteJob;
import be.bagofwords.distributed.test.TestJobWithoutSpringContext;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.DataIterable;
import be.bagofwords.main.ComputingClientMain;
import be.bagofwords.ui.UI;
import org.junit.Test;

public class TestRemoteJobWithoutSpringContext {

    @Test
    public void testRemoteJobService() {

        long start = System.currentTimeMillis();
        final int numOfJobs = 100;

        RemoteJobService remoteJobService = new RemoteJobService();


        for (int i = 0; i < 10; i++) {
            new Thread() {
                public void run() {
                    new ComputingClientMain().main(new String[0]);
                }
            }.start();
        }

        remoteJobService.runRemoteJobs("testJob", new DataIterable<Integer>() {
                    @Override
                    public CloseableIterator<Integer> iterator() {
                        return new CloseableIterator<Integer>() {

                            int counter;

                            @Override
                            protected void closeInt() {
                                //OK
                            }

                            @Override
                            public boolean hasNext() {
                                return counter < numOfJobs;
                            }

                            @Override
                            public Integer next() {
                                return counter++;
                            }
                        };
                    }

                    @Override
                    public long apprSize() {
                        return numOfJobs;
                    }
                }, new RemoteJobFactory<Integer>() {
                    @Override
                    public RemoteJob createRemoteJob(Integer numberOfIterations) {
                        return new TestJobWithoutSpringContext(10);
                    }

                    @Override
                    public void remoteJobFinished(RemoteJob remoteJob) {
                        //OK
                    }
                }
        );
        UI.write("Total time taken is " + (System.currentTimeMillis() - start));
        remoteJobService.terminate();
    }

}
