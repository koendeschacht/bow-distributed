package be.bagofwords.distributed.shared;

public class ProtocolConstants {

    public static final byte REQUEST_JOB = 5;

    public static final byte SENDING_JOB_TO_COMPUTING_CLIENT = 1;

    public static final byte SENDING_JOB_BACK_TO_SERVER = 2;

    public static final byte ASKING_OBJECT_FROM_SERVER = 7;

    public static final byte SENDING_OBJECT_TO_COMPUTING_CLIENT = 9;

    public static final byte JOB_IS_TERMINATED = 14;

    public static final byte READ_CLASS_DATAS = 3;

    public static final byte SENDING_CLASS_DATAS_TO_CLIENT = 4;

    public static final byte READ_CONTEXT_FACTORY_CLASS = 6;

    public static final byte SEND_CONTEXT_FACTORY_CLASS = 8;
}
