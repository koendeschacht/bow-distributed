package be.bagofwords.distributed.computingClient;

import be.bagofwords.ui.UI;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

public class NetworkedClassLoader extends ClassLoader {

    private final ComputingClientConnection connection;
    private final ClassLoader parent;
    private final Set<String> notFoundClasses; //these are not cached by the parent loader

    public NetworkedClassLoader(ClassLoader parent, ComputingClientConnection connection) {
        super(parent);
        this.connection = connection;
        this.parent = parent;
        this.notFoundClasses = new HashSet<>();
    }

    public Class loadClass(String name) throws ClassNotFoundException {
        Class class_;
        try {
            class_ = parent.loadClass(name);
        } catch (ClassNotFoundException exp) {
            class_ = findLoadedClass(name);
            if (class_ == null) {
                if (notFoundClasses.contains(name)) {
                    throw new ClassNotFoundException("Class " + name + " was not found.");
                } else {
                    try {
                        byte[] b = loadClassData(name);
                        class_ = super.defineClass(name, b, 0, b.length);
                    } catch (IOException e) {
                        notFoundClasses.add(name);
                        throw new ClassNotFoundException("Class " + name + " was not found.");
                    }
                }
            }
        }
        return class_;
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        InputStream in = super.getResourceAsStream(name);
        if (in != null) {
            return in;
        }
        if (name.endsWith(".class")) {
            //Try to load class from network:
            try {
                byte[] classData = loadClassData(name.substring(0, name.length() - ".class".length()));
                return new ByteArrayInputStream(classData);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load class data " + name, e);
            } catch (ClassNotFoundException e) {
                return null;
            }
        } else {
            return null;
        }
    }

    private byte[] loadClassData(String name) throws IOException, ClassNotFoundException {
        UI.write("-->Asking for class " + name);
        byte[] data = connection.readClassData(name);
        if (data != null) {
            return data;
        } else {
            throw new ClassNotFoundException("Class could not be found through the JobServer. Maybe there is a network problem?");
        }
    }
}
