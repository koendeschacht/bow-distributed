package be.bagofwords.distributed.test;

import be.bagofwords.application.annotations.BowComponent;
import be.bagofwords.distributed.shared.RemoteJob;
import org.springframework.beans.factory.annotation.Autowired;

@BowComponent
public class TestJobWithSpringContext extends RemoteJob {

    private double result;
    private int numberOfIterations;

    @Autowired
    private DummyComponent dummyComponent;

    public TestJobWithSpringContext(int numberOfIterations) {
        super("TestJob");
        this.numberOfIterations = numberOfIterations;
    }

    public void execute() throws Exception {
        for (int i = 0; i < numberOfIterations; i++) {
            for (int j = 0; j < 500; j++) {
                result = Math.pow(dummyComponent.getRandomNumber(), 0.3);
            }
        }
    }

    public double getResult() {
        return result;
    }

    //Serialization

    public TestJobWithSpringContext() {
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
