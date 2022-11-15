package cn.xdf.acdc.devops.service.utility.i18n;

public final class I18nKey {

    public static final String SERVER_INTERNAL_ERROR = "server.internal-error";

    public final class Authorization {

        public static final String INVALID_USER = "authorization.invalid-user";
    }

    public final class Client {

        public static final String INVALID_PARAMETER = "client.invalid-parameter";
    }

    public final class User {

        public static final String NOT_FOUND = "user.not-found";

        public static final String ERROR_ROLE = "user.error-role";

        public static final String ERROR_ORIGINAL_PASSWORD = "user.error-original-password";
    }

    public final class Kafka {

        public static final String CLUSTER_ALREADY_EXISTED = "kafka.cluster-already-existed";

        public static final String CLUSTER_NOT_FOUND = "kafka.cluster-not-found";
        // cluster-delete-cluster-with-connectors

        public static final String CLUSTER_DELETE_CLUSTER_WITH_CONNECTORS = "kafka.cluster-delete-cluster-with-connectors";
    }

    public final class Connect {

        public static final String CLUSTER_NOT_FOUND = "connect.cluster-not-found";

        public static final String CLUSTER_ALREADY_EXISTED = "connect.cluster-already-existed";

        // delete-cluster-with-connectors
        public static final String CLUSTER_DELETE_CLUSTER_WITH_CONNECTORS = "connect.cluster-delete-cluster-with-connectors";

        public static final String CLUSTER_DEFAULT_CONFIG_NOT_FOUND = "connect.cluster-default-config-not-found";

        public static final String CLUSTER_DEFAULT_CONFIG_ERROR_MODIFICATION = "connect.cluster-default-config-error-modification";
    }

    public final class Command {

        public static final String EXECUTION_FAILED = "command.execution-failed";

        public static final String OPERATION_NOT_SPECIFIED = "command.operation_not_specified";
    }
}
