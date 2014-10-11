package be.bagofwords.distributed.jobServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class ClassReader {

    public static byte[] getClass(String fullName) {
        ClassLoader loader = ClassLoader.getSystemClassLoader();
        String fileName = fullName.replaceAll("\\.", System.getProperty("file.separator"));
        fileName += ".class";
        URL urlOfClass = loader.getResource(fileName);
        if (urlOfClass == null)
            return null;
        else {
            try {
                File classFile = new File(urlOfClass.getFile());
                InputStream is = new FileInputStream(classFile);
                long length = classFile.length();
                if (length > Integer.MAX_VALUE) {
                    throw new IOException("The file " + fullName + " is too long!");
                } else {
                    byte[] classBytes = new byte[(int) length];
                    int offset = 0;
                    int numRead = 0;
                    while (offset < classBytes.length && (numRead = is.read(classBytes, offset, classBytes.length - offset)) >= 0) {
                        offset += numRead;
                    }
                    if (offset < classBytes.length) {
                        throw new IOException("Could not completely read " + fullName);
                    }
                    return classBytes;
                }
            } catch (IOException exp) {
                return null;
            }
        }
    }

}
