package jp.openstandia.midpoint.grpc;

import com.evolveum.midpoint.model.api.ModelService;
import com.evolveum.midpoint.model.api.authentication.GuiProfiledPrincipal;
import com.evolveum.midpoint.model.api.context.EvaluatedAssignment;
import com.evolveum.midpoint.model.impl.lens.AssignmentCollector;
import com.evolveum.midpoint.model.impl.security.SecurityHelper;
import com.evolveum.midpoint.prism.PrismContext;
import com.evolveum.midpoint.prism.PrismObject;
import com.evolveum.midpoint.prism.polystring.PolyString;
import com.evolveum.midpoint.prism.query.ObjectQuery;
import com.evolveum.midpoint.schema.result.OperationResult;
import com.evolveum.midpoint.schema.util.ObjectQueryUtil;
import com.evolveum.midpoint.security.api.*;
import com.evolveum.midpoint.security.enforcer.api.AuthorizationParameters;
import com.evolveum.midpoint.security.enforcer.api.SecurityEnforcer;
import com.evolveum.midpoint.task.api.Task;
import com.evolveum.midpoint.task.api.TaskManager;
import com.evolveum.midpoint.util.exception.*;
import com.evolveum.midpoint.util.logging.Trace;
import com.evolveum.midpoint.util.logging.TraceManager;
import com.evolveum.midpoint.web.boot.GrpcServerConfiguration;
import com.evolveum.midpoint.xml.ns._public.common.common_3.AuthorizationType;
import com.evolveum.midpoint.xml.ns._public.common.common_3.FocusType;
import com.evolveum.midpoint.xml.ns._public.common.common_3.UserType;
import io.grpc.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

import static jp.openstandia.midpoint.grpc.MidPointGrpcService.CHANNEL_GRPC_SERVICE_URI;
import static jp.openstandia.midpoint.grpc.MidPointGrpcService.OPERATION_GRPC_SERVICE;

@Component
public abstract class AbstractGrpcAuthenticationInterceptor implements ServerInterceptor {

    private static final Trace LOGGER = TraceManager.getTrace(AbstractGrpcAuthenticationInterceptor.class);

    // https://tools.ietf.org/html/rfc6750#section-3.1
    protected static final String INVALID_REQUEST = "invalid_request"; // INVALID_ARGUMENT (400)
    protected static final String INVALID_TOKEN = "invalid_token"; // UNAUTHENTICATED (401)
    protected static final String INSUFFICIENT_SCOPE = "insufficient_scope"; // PERMISSION_DENIED (403)

    protected static final String INTERNAL_ERROR = "internal_error"; // INTERNAL (500)

    private final String opNamePrefix = getClass().getName() + ".";

    @Autowired
    PrismContext prismContext;

    @Autowired
    ModelService modelService;

    @Autowired
    SecurityEnforcer securityEnforcer;

    @Autowired
    SecurityHelper securityHelper;

    @Autowired
    TaskManager taskManager;

    @Autowired
    AssignmentCollector assignmentCollector;

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        LOGGER.trace("Start interceptCall");
        try {
            return doProcess(call, headers, next);
        } catch (StatusRuntimeException e) {
            Metadata metadata = e.getTrailers();
            if (metadata == null) {
                metadata = new Metadata();
            }
            call.close(e.getStatus(), metadata);

            // https://github.com/grpc/grpc-java/issues/2814
            return new ServerCall.Listener() {
            };
        } finally {
            LOGGER.trace("End interceptCall");
        }
    }

    protected <ReqT, RespT> ServerCall.Listener<ReqT> doProcess(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        // How to get remote address/port
        // https://github.com/grpc/grpc-java/blob/30b59885b7496b53eb17f64ba1d822c2d9a6c69a/interop-testing/src/main/java/io/grpc/testing/integration/AbstractInteropTest.java#L1627-L1639

        final String remoteInetSocketString = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString();
        final String localInetSocketString = call.getAttributes().get(Grpc.TRANSPORT_ATTR_LOCAL_ADDR).toString();

        HttpConnectionInformation connection = new HttpConnectionInformation();
        connection.setRemoteHostAddress(remoteInetSocketString);
        connection.setLocalHostName(localInetSocketString);

        LOGGER.trace("Authenticating to gRPC service");

        // We need to create task before attempting authentication. Task ID is also a session ID.
        final Task task = GrpcServerConfiguration.getApplication().getTaskManager().createTaskInstance(OPERATION_GRPC_SERVICE);
        task.setChannel(CHANNEL_GRPC_SERVICE_URI);

        connection.setSessionId(task.getTaskIdentifier());
        ConnectionEnvironment connEnv = new ConnectionEnvironment(CHANNEL_GRPC_SERVICE_URI, connection);

        // Client authentication by Authorization header
        String header = headers.get(Constant.AuthorizationMetadataKey);

        if (header == null || !header.toLowerCase().startsWith(getType().toLowerCase() + " ")) {
            throw Status.INVALID_ARGUMENT
                    .withDescription(INVALID_REQUEST)
                    .asRuntimeException();
        }

        Authentication auth = authenticate(connEnv, task, header);

        // Check authorization for gRPC service
        authorizeClient(auth, connEnv, task);

        // Switch user and run privileged if requested
        boolean runPrivileged = Boolean.parseBoolean(headers.get(Constant.RunPrivilegedMetadataKey));
        auth = switchToUser(auth, headers, runPrivileged, connEnv, task);

        FocusType user = ((MidPointPrincipal) auth.getPrincipal()).getFocus();
        task.setOwner(user.asPrismObject());

        // Run Privileged
        if (runPrivileged) {
            auth = runPrivileged(auth);
        }

        OperationResult result = task.getResult().createSubresult("grpcService");

        Context ctx = Context.current()
                .withValue(ServerConstant.ConnectionContextKey, connection)
                .withValue(ServerConstant.ConnectionEnvironmentContextKey, connEnv)
                .withValue(ServerConstant.TaskContextKey, task)
                .withValue(ServerConstant.AuthenticationContextKey, auth)
                .withValue(ServerConstant.AuthorizationHeaderContextKey, header);

        ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT> serverCall = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                if (!status.isOk()) {
                    switch (status.getCode()) {
                        case INVALID_ARGUMENT:
                        case UNAUTHENTICATED:
                        case NOT_FOUND:
                        case ALREADY_EXISTS:
                        case FAILED_PRECONDITION:
                        case ABORTED:
                        case OUT_OF_RANGE:
                        case PERMISSION_DENIED:
                            LOGGER.info("Error in calling gRPC service. status={}, metadata={}", status, trailers);
                            break;
                        default:
                            LOGGER.error("Error in calling gRPC service. status={}, metadata={}", status, trailers);
                            break;
                    }
                }
                // TODO Check REST API implementation
//                task.setOwner(user.asPrismObject());
                finishRequest(task, connEnv, result);
                super.close(status, trailers);
            }
        };

        return Contexts.interceptCall(ctx, serverCall, headers, next);
    }

    protected OperationResult createSubresult(Task task, String operation) {
        return task.getResult().createSubresult(opNamePrefix + operation);
    }

    private Authentication runPrivileged(Authentication origAuthentication) {
        LOGGER.debug("Running gRPC service as privileged");
        LOGGER.trace("ORIG auth {}", origAuthentication);
        Authorization privilegedAuthorization = createPrivilegedAuthorization();
        Object newPrincipal = null;
        Object origPrincipal;

        origPrincipal = origAuthentication.getPrincipal();

        LOGGER.trace("ORIG principal {} ({})", origPrincipal, origPrincipal != null ? origPrincipal.getClass() : null);
        MidPointPrincipal newMidPointPrincipal = ((MidPointPrincipal) origPrincipal).clone();
        newMidPointPrincipal.getAuthorities().add(privilegedAuthorization);
        newPrincipal = newMidPointPrincipal;

        Collection<GrantedAuthority> newAuthorities = new ArrayList();
        newAuthorities.add(privilegedAuthorization);
        PreAuthenticatedAuthenticationToken newAuthorization = new PreAuthenticatedAuthenticationToken(newPrincipal, (Object) null, newAuthorities);
        LOGGER.trace("NEW auth {}", newAuthorization);

        return newAuthorization;
    }

    private Authorization createPrivilegedAuthorization() {
        AuthorizationType authorizationType = new AuthorizationType();
        authorizationType.getAction().add(AuthorizationConstants.AUTZ_ALL_URL);
        return new Authorization(authorizationType);
    }

    protected abstract String getType();

    protected abstract Authentication authenticate(ConnectionEnvironment connEnv, Task task, String header);

    protected abstract void authorizeClient(Authentication auth, ConnectionEnvironment connEnv, Task task);

    protected abstract Authentication switchToUser(Authentication auth, Metadata headers, boolean runPrivileged, ConnectionEnvironment connEnv, Task task);

    protected Authentication authenticateSwitchUser(PrismObject<? extends FocusType> user, boolean runPrivileged, ConnectionEnvironment connEnv, Task task) {
        try {
            // Don't use securityContextManager.setupPreAuthenticatedSecurityContext(user) here because
            // it sets the authentication into thread-local area.

            // Don't use UserProfileService#getPrincipal(...) here due to high processing costs for compiling user profile for GUI.
            // We simply create MidPointUserProfilePrincipal here because user profile for GUI is not needed for gRPC.
            GuiProfiledPrincipal principal = new GuiProfiledPrincipal(user.asObjectable());

            // Don't need to collect authorization if running privileged mode
            if (!runPrivileged) {
                Collection<? extends EvaluatedAssignment<? extends FocusType>> evaluatedAssignments = assignmentCollector.collect(user, true, task, task.getResult());
                Collection<Authorization> authorizations = principal.getAuthorities();
                for (EvaluatedAssignment<? extends FocusType> assignment : evaluatedAssignments) {
                    if (assignment.isValid()) {
                        for (Authorization autz : assignment.getAuthorizations()) {
                            authorizations.add(autz.clone());
                        }
                    }
                }
            }

            PreAuthenticatedAuthenticationToken token = new PreAuthenticatedAuthenticationToken(principal, null, principal.getAuthorities());

            LOGGER.trace("Switch authenticated to gRPC service as {}", user);

            return token;
        } catch (SchemaException e) {
            securityHelper.auditLoginFailure(user.getName().getOrig(), user.asObjectable(), connEnv, "Schema error: " + e.getMessage());
            StatusRuntimeException exception = Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asRuntimeException();
            throw exception;
        }
    }

    protected void authorizeUser(Authentication auth, String authorization, FocusType user, PrismObject<? extends FocusType> proxyUser, ConnectionEnvironment connEnv) {
        Task task = taskManager.createTaskInstance(AbstractGrpcAuthenticationInterceptor.class.getName() + ".authorizeUser");
        try {
            // SecurityEnforcer#authorize needs authentication in SecurityContext.
            SecurityContextHolder.getContext().setAuthentication(auth);

            // authorize for proxy
            securityEnforcer.authorize(authorization, null, AuthorizationParameters.Builder.buildObject(proxyUser), null, task, task.getResult());
        } catch (SecurityViolationException e) {
            securityHelper.auditLoginFailure(user.getName().getOrig(), user, connEnv, "Not authorized");
            throw Status.PERMISSION_DENIED
                    .withDescription(e.getMessage())
                    .asRuntimeException();
        } catch (SchemaException | ObjectNotFoundException | ExpressionEvaluationException | CommunicationException | ConfigurationException e) {
            securityHelper.auditLoginFailure(user.getName().getOrig(), user, connEnv, "Internal error: " + e.getMessage());
            throw Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asRuntimeException();
        } finally {
            SecurityContextHolder.getContext().setAuthentication(null);
        }
    }

    protected PrismObject<? extends FocusType> findByOid(Authentication auth, String oid, Task task) {
        OperationResult result = task.getResult();
        try {
            SecurityContextHolder.getContext().setAuthentication(auth);

            PrismObject<UserType> user = modelService.getObject(UserType.class, oid, null, task, result);
            return user;
        } catch (SchemaException | ObjectNotFoundException | SecurityViolationException | CommunicationException | ConfigurationException | ExpressionEvaluationException e) {
            LOGGER.trace("Exception while authenticating user identified with oid: '{}' to gRPC service: {}", oid, e.getMessage(), e);
            throw Status.UNAUTHENTICATED
                    .withDescription(e.getMessage())
                    .asRuntimeException();
        } finally {
            SecurityContextHolder.getContext().setAuthentication(null);
        }
    }

    protected PrismObject<? extends FocusType> findByUsername(Authentication auth, String username, Task task) {
        OperationResult result = task.getResult();
        try {
            SecurityContextHolder.getContext().setAuthentication(auth);

            PolyString usernamePoly = new PolyString(username);
            ObjectQuery query = ObjectQueryUtil.createNormNameQuery(usernamePoly, prismContext);
            LOGGER.trace("Looking for user, query:\n" + query.debugDump());

            List<PrismObject<UserType>> list = GrpcServerConfiguration.getApplication().getRepositoryService().searchObjects(UserType.class, query, null, result);
            LOGGER.trace("Users found: {}.", list.size());
            if (list.size() != 1) {
                throw Status.UNAUTHENTICATED
                        .withDescription("Not found user")
                        .asRuntimeException();
            }
            return list.get(0);
        } catch (SchemaException e) {
            LOGGER.trace("Exception while authenticating user identified with name: '{}' to gRPC service: {}", username, e.getMessage(), e);
            throw Status.UNAUTHENTICATED
                    .withDescription(e.getMessage())
                    .asRuntimeException();
        } finally {
            SecurityContextHolder.getContext().setAuthentication(null);
        }
    }

    protected String extractHeader(String header, String type) {
        return header.substring(type.length() + 1);
    }

    protected String extractAndDecodeHeader(String header, String type) {
        try {
            byte[] decoded;
            try {
                // Browsers uses UTF-8
                // https://bugzilla.mozilla.org/show_bug.cgi?id=1419658
                decoded = Base64.getDecoder().decode(extractHeader(header, type).getBytes("UTF-8"));
            } catch (IllegalArgumentException e) {
                LOGGER.warn("Failed to base64 decode grpc authorization header: {}", header, e);

                throw Status.UNAUTHENTICATED
                        .withDescription("Failed to decode basic authentication token")
                        .asRuntimeException();
            }

            String token = new String(decoded, "UTF-8");

            return token;

        } catch (UnsupportedEncodingException e) {
            LOGGER.warn("Failed to decode grpc authorization header: {}", header, e);

            throw Status.INVALID_ARGUMENT
                    .withDescription(INVALID_REQUEST)
                    .asRuntimeException();
        }
    }

    protected void finishRequest(Task task, ConnectionEnvironment connEnv, OperationResult result) {
        task.getResult().computeStatus();
        connEnv.setSessionIdOverride(task.getTaskIdentifier());
        GrpcServerConfiguration.getSecurityHelper().auditLogout(connEnv, task, result);
    }
}
