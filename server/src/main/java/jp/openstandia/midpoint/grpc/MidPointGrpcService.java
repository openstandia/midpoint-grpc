package jp.openstandia.midpoint.grpc;

import com.evolveum.midpoint.security.api.ConnectionEnvironment;
import com.evolveum.midpoint.security.api.HttpConnectionInformation;
import com.evolveum.midpoint.security.api.MidPointPrincipal;
import com.evolveum.midpoint.task.api.Task;
import com.evolveum.midpoint.util.exception.*;
import com.evolveum.midpoint.util.logging.Trace;
import com.evolveum.midpoint.util.logging.TraceManager;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.servlet.http.HttpServletRequestWrapper;
import org.lognet.springboot.grpc.recovery.GRpcExceptionHandler;
import org.lognet.springboot.grpc.recovery.GRpcExceptionScope;
import org.lognet.springboot.grpc.recovery.GRpcServiceAdvice;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.xml.namespace.QName;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static com.evolveum.midpoint.schema.constants.SchemaConstants.NS_CHANNEL;
import static com.evolveum.midpoint.util.QNameUtil.qNameToUri;

public interface MidPointGrpcService {
    static final Trace LOGGER = TraceManager.getTrace(MidPointGrpcService.class);

    public static final String CLASS_DOT = MidPointGrpcService.class.getName() + ".";
    public static final String OPERATION_GRPC_SERVICE = CLASS_DOT + "grpcService";

    public static final String CHANNEL_GRPC_LOCAL = "grpc";
    public static final QName CHANNEL_GRPC_QNAME = new QName(NS_CHANNEL, CHANNEL_GRPC_LOCAL);
    public static final String CHANNEL_GRPC_SERVICE_URI = qNameToUri(CHANNEL_GRPC_QNAME);

    default <T> T runTask(MidPointTask<T> task) {
        Authentication auth = ServerConstant.AuthenticationContextKey.get();
        SecurityContextHolder.getContext().setAuthentication(auth);

        HttpConnectionInformation connection = ServerConstant.ConnectionContextKey.get();
        ConnectionEnvironment connEnv = ServerConstant.ConnectionEnvironmentContextKey.get();
        Task t = ServerConstant.TaskContextKey.get();

        MidPointPrincipal principal = (MidPointPrincipal) auth.getPrincipal();

        // To record some request information in audit log
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(new DummyRequest(connection)));

        try {
            T result = task.run(new MidPointTaskContext(connection, connEnv, t, auth, principal));
            return result;
        } catch (Exception e) {
            throw new GrpcServiceException(e);
        } finally {
            RequestContextHolder.resetRequestAttributes();
            SecurityContextHolder.getContext().setAuthentication(null);
        }
    }

    default void handlePolicyViolationException(Metadata responseHeaders, PolicyViolationException e) {
    }

    /**
     * grpc-spring-boot-starter v5.0 depends on jakarta.validation.
     * Before v5.0, it depends on javax.validation.
     * Since javax.validation can be used in midPoint, org.lognet.springboot.grpc.autoconfigure.GRpcValidationConfiguration
     * is registered as global gRPC Exception Handler.
     * From v5.0, we need to register own global gRPC Exception handler as dummy because of missing jakarta.validation.
     */
    @GRpcServiceAdvice
    static class DummyErrorHandler {
        @GRpcExceptionHandler
        public Status handle(DummyException e, GRpcExceptionScope scope) {
            return Status.UNKNOWN;
        }

        static class DummyException extends RuntimeException {
        }
    }

    @GRpcExceptionHandler
    default Status handleException(GrpcServiceException gse, GRpcExceptionScope scope) {
        Throwable cause = gse.getCause();

        if (cause instanceof ObjectNotFoundException) {
            ObjectNotFoundException e = (ObjectNotFoundException) cause;
            return Status.NOT_FOUND
                    .withDescription(e.getErrorTypeMessage())
                    .withCause(e);
        }
        if (cause instanceof PolicyViolationException) {
            PolicyViolationException e = (PolicyViolationException) cause;
            handlePolicyViolationException(scope.getResponseHeaders(), e);
            return Status.INVALID_ARGUMENT
                    .withDescription(e.getErrorTypeMessage())
                    .withCause(e);
        }
        if (cause instanceof SchemaException) {
            SchemaException e = (SchemaException) cause;
            return Status.INVALID_ARGUMENT
                    .withDescription(e.getErrorTypeMessage())
                    .withCause(e);
        }
        if (cause instanceof ExpressionEvaluationException) {
            ExpressionEvaluationException e = (ExpressionEvaluationException) cause;
            return Status.INVALID_ARGUMENT
                    .withDescription(e.getErrorTypeMessage())
                    .withCause(e);
        }
        if (cause instanceof AuthorizationException) {
            AuthorizationException e = (AuthorizationException) cause;
            return Status.PERMISSION_DENIED
                    .withDescription(e.getErrorTypeMessage())
                    .withCause(e);
        }
        if (cause instanceof SecurityViolationException) {
            SecurityViolationException e = (SecurityViolationException) cause;
            return Status.PERMISSION_DENIED
                    .withDescription(e.getErrorTypeMessage())
                    .withCause(e);
        }
        if (cause instanceof ObjectAlreadyExistsException) {
            ObjectAlreadyExistsException e = (ObjectAlreadyExistsException) cause;
            return Status.ALREADY_EXISTS
                    .withDescription(e.getErrorTypeMessage())
                    .withCause(e);
        }
        if (cause instanceof StatusRuntimeException) {
            return ((StatusRuntimeException) cause).getStatus();
        }
        if (cause instanceof Exception) {
            return Status.INTERNAL
                    .withDescription(cause.getMessage())
                    .withCause(cause);
        }

        return Status.INTERNAL;
    }

    // Based on org.springframework.security.web.FilterInvocation.DummyRequest
    static class DummyRequest extends HttpServletRequestWrapper {

        private static final HttpServletRequest UNSUPPORTED_REQUEST = (HttpServletRequest) Proxy.newProxyInstance(
                DummyRequest.class.getClassLoader(), new Class[]{HttpServletRequest.class},
                new UnsupportedOperationExceptionInvocationHandler());

        private final HttpConnectionInformation connection;

        DummyRequest(HttpConnectionInformation connection) {
            super(UNSUPPORTED_REQUEST);
            this.connection = connection;
        }

        @Override
        public HttpSession getSession(boolean create) {
            return null;
        }

        @Override
        public String getServerName() {
            return connection.getServerName();
        }

        @Override
        public String getLocalName() {
            return connection.getLocalHostName();
        }

        @Override
        public String getRemoteAddr() {
            return connection.getRemoteHostAddress();
        }
    }

    static final class UnsupportedOperationExceptionInvocationHandler implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.isDefault()) {
                return invokeDefaultMethod(proxy, method, args);
            }
            throw new UnsupportedOperationException(method + " is not supported");
        }

        private Object invokeDefaultMethod(Object proxy, Method method, Object[] args) throws Throwable {
            return MethodHandles.lookup()
                    .findSpecial(method.getDeclaringClass(), method.getName(),
                            MethodType.methodType(method.getReturnType(), new Class[0]), method.getDeclaringClass())
                    .bindTo(proxy).invokeWithArguments(args);
        }
    }
}