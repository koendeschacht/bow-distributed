package be.bagofwords.distributed.test;

import be.bagofwords.application.annotations.BowComponent;
import be.bagofwords.distributed.shared.RemoteJob;

@BowComponent
public class TestJobWithoutSpringContext extends RemoteJob {

    private double result;
    private int numberOfIterations;

    public TestJobWithoutSpringContext(int numberOfIterations) {
        super("TestJob");
        this.numberOfIterations = numberOfIterations;
    }

    public void execute() throws Exception {
        for (int i = 0; i < numberOfIterations; i++) {
            for (int j = 0; j < 5000; j++) {
                result = Math.pow(Math.random(), 0.3);
            }
        }
    }

    public double getResult() {
        return result;
    }

    //Serialization

    public TestJobWithoutSpringContext() {
    }

    public void setResult(double result) {
        this.result = result;
    }

    public int getNumberOfIterations() {
        return numberOfIterations;
    }

    public void setNumberOfIterations(int numberOfIterations) {
        this.numberOfIterations = numberOfIterations;
    }

}
