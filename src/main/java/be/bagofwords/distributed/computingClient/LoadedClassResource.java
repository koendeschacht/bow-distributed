package be.bagofwords.distributed.computingClient;

import org.springframework.core.io.AbstractResource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class LoadedClassResource extends AbstractResource {

    private String name;
    private byte[] data;

    public LoadedClassResource(String name, byte[] data) {
        this.name = name;
        this.data = data;
    }

    @Override
    public String getDescription() {
        return name;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return new ByteArrayInputStream(data);
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public long contentLength() throws IOException {
        return data.length;
    }

}
