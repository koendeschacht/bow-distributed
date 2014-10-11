package be.bagofwords.distributed.jobServer;

import be.bagofwords.distributed.shared.RemoteJob;

public interface RemoteJobFactory<T> {

    public RemoteJob createRemoteJob(T value);

    public void remoteJobFinished(RemoteJob remoteJob);

}
