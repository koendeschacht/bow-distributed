package be.bagofwords.distributed.computingClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public class ObjectInputStreamUsingNetworkedClassLoading extends ObjectInputStream {

    private NetworkedClassLoader nCl;

    public ObjectInputStreamUsingNetworkedClassLoading(NetworkedClassLoader cl) throws IOException, SecurityException {
        super();
        this.nCl = cl;
    }

    public ObjectInputStreamUsingNetworkedClassLoading(InputStream in, NetworkedClassLoader cl) throws IOException {
        super(in);
        this.nCl = cl;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        try {
            Class<?> class_ = super.resolveClass(desc);
            return class_;
        } catch (ClassNotFoundException exp) {
            return Class.forName(desc.getName(), false, nCl);
        }
    }

}
