package be.bagofwords.distributed.computingClient;

import be.bagofwords.util.Pair;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.io.IOException;
import java.util.List;

public class NetworkedResourceLoader implements ResourcePatternResolver {

    private final ComputingClientConnection connection;

    public NetworkedResourceLoader(ComputingClientConnection connection) {
        this.connection = connection;
    }

    @Override
    public Resource[] getResources(String locationPattern) throws IOException {
        List<Pair<String, byte[]>> data = connection.readClassDatas(locationPattern);
        Resource[] result = new Resource[data.size()];
        for (int i = 0; i < data.size(); i++) {
            result[i] = new LoadedClassResource(data.get(i).getFirst(), data.get(i).getSecond());
        }
        return result;
    }

    @Override
    public Resource getResource(String location) {
        try {
            byte[] data = connection.readClassData(location);
            if (data != null) {
                return new LoadedClassResource(location, data);
            } else {
                throw new RuntimeException("Could not load " + location);
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not load " + location, e);
        }
    }

    @Override
    public ClassLoader getClassLoader() {
        return connection.getNetworkedClassLoader();
    }
}
