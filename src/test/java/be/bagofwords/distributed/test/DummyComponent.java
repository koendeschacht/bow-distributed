package be.bagofwords.distributed.test;

import be.bagofwords.application.annotations.BowComponent;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/11/14.
 */

@BowComponent
public class DummyComponent {

    public double getRandomNumber() {
        return Math.random();
    }

}
