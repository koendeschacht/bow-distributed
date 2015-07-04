package be.bagofwords.distributed;


import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.distributed.jobServer.RemoteJobFactory;
import be.bagofwords.distributed.jobServer.RemoteJobService;
import be.bagofwords.distributed.shared.RemoteJob;
import be.bagofwords.distributed.test.TestJobWithSpringContext;
import be.bagofwords.distributed.test.UnitTestContextLoader;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.DataIterable;
import be.bagofwords.main.ComputingClientMain;
import be.bagofwords.ui.UI;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(value = SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = UnitTestContextLoader.class)
public class TestRemoteJobServiceWithSpringContext {

    @Autowired
    private RemoteJobService remoteJobService;
    @Autowired
    private MemoryManager memoryManager;

    @Test
    public void testRemoteJobService() {
        memoryManager.setDumpHeapToFileWhenMemoryFull(true);
        long start = System.currentTimeMillis();
        final int numOfJobs = 100;

        for (int i = 0; i < 10; i++) {
            new Thread() {
                public void run() {
                    ComputingClientMain.main(new String[0]);
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
                        return new TestJobWithSpringContext(10);
                    }

                    @Override
                    public void remoteJobFinished(RemoteJob remoteJob) {
                        //OK
                    }
                }
        );
        UI.write("Total time taken is " + (System.currentTimeMillis() - start));
    }

    @After
    public void closeJobService() {
        remoteJobService.terminate();
    }

}
