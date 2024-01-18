package jp.openstandia.midpoint.grpc;

import com.evolveum.midpoint.authentication.api.evaluator.AuthenticationEvaluator;
import com.evolveum.midpoint.authentication.api.evaluator.context.PasswordAuthenticationContext;
import com.evolveum.midpoint.authentication.impl.FocusAuthenticationResultRecorder;
import com.evolveum.midpoint.prism.PrismObject;
import com.evolveum.midpoint.security.api.AuthorizationConstants;
import com.evolveum.midpoint.security.api.ConnectionEnvironment;
import com.evolveum.midpoint.security.api.MidPointPrincipal;
import com.evolveum.midpoint.task.api.Task;
import com.evolveum.midpoint.util.logging.Trace;
import com.evolveum.midpoint.util.logging.TraceManager;
import com.evolveum.midpoint.xml.ns._public.common.common_3.FocusType;
import com.evolveum.midpoint.xml.ns._public.common.common_3.UserType;
import io.grpc.Metadata;
import io.grpc.Status;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.stereotype.Component;

@Component
public class BasicAuthenticationInterceptor extends AbstractGrpcAuthenticationInterceptor {

    private static final Trace LOGGER = TraceManager.getTrace(BasicAuthenticationInterceptor.class);
    private static final String TYPE = "Basic";

    @Autowired
    transient AuthenticationEvaluator<PasswordAuthenticationContext, UsernamePasswordAuthenticationToken> passwordAuthenticationEvaluator;

    @Autowired
    FocusAuthenticationResultRecorder authenticationRecorder;

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Authentication authenticate(ConnectionEnvironment connEnv, Task task, String header) {
        String[] tokens = extractAndDecodeBasicAuthzHeader(header);

        Authentication authToken = authenticateUser(connEnv, tokens[0], tokens[1]);

        return authToken;
    }

    @Override
    protected void authorizeClient(Authentication auth, ConnectionEnvironment connEnv, Task task) {
        MidPointPrincipal p = (MidPointPrincipal) auth.getPrincipal();
        FocusType user = p.getFocus();
        authorizeUser(auth, AuthorizationConstants.AUTZ_REST_ALL_URL, user, null, connEnv);
    }

    @Override
    protected Authentication switchToUser(Authentication auth, Metadata headers, boolean runPrivileged, ConnectionEnvironment connEnv, Task task) {
        String switchUser = headers.get(Constant.SwitchToPrincipalMetadataKey);
        String switchUserByName = headers.get(Constant.SwitchToPrincipalByNameMetadataKey);

        // Find proxy user
        PrismObject<? extends FocusType> authorizedUser;
        if (StringUtils.isNotBlank(switchUser)) {
            authorizedUser = findByOid(auth, switchUser, task);
        } else if (StringUtils.isNotBlank(switchUserByName)) {
            authorizedUser = findByUsername(auth, switchUserByName, task);
        } else {
            // No switching
            return auth;
        }

        // Authorization proxy user
        FocusType client = ((MidPointPrincipal) auth.getPrincipal()).getFocus();
        authorizeUser(auth, AuthorizationConstants.AUTZ_REST_PROXY_URL, client, authorizedUser, connEnv);

        return authenticateSwitchUser(authorizedUser, runPrivileged, connEnv, task);
    }

    protected String[] extractAndDecodeBasicAuthzHeader(String header) {
        String token = extractAndDecodeHeader(header, "basic");

        int delim = token.indexOf(":");

        if (delim == -1) {
            throw Status.UNAUTHENTICATED
                    .withDescription("Invalid basic authentication token")
                    .asRuntimeException();
        }
        return new String[]{token.substring(0, delim), token.substring(delim + 1)};
    }

    private UsernamePasswordAuthenticationToken authenticateUser(ConnectionEnvironment connEnv, String username, String password) {
        LOGGER.debug("Start authenticateUser: {}", username);
        UsernamePasswordAuthenticationToken token = null;
        try {
            // login session is recorded here
            // TODO Use custom evaluator here because it takes several tens of ms
            connEnv.setSequenceIdentifier("grpc");
            connEnv.setModuleIdentifier("httpBasic");
            token = passwordAuthenticationEvaluator.authenticate(connEnv, new PasswordAuthenticationContext(username, password, UserType.class));
            return token;
        } catch (AuthenticationException ex) {
            LOGGER.info("Not authenticated. user: {}, reason: {}", username, ex.getMessage());
            throw Status.UNAUTHENTICATED
                    .withDescription("invalid_token")
                    .withCause(ex)
                    .asRuntimeException();
        } finally {
            writeRecord(connEnv, token, username);
            LOGGER.debug("End authenticateUser: {}", username);
        }
    }

    // Based on com.evolveum.midpoint.authentication.impl.filter.SequenceAuditFilter#writeRecord
    private void writeRecord(ConnectionEnvironment connEnv, UsernamePasswordAuthenticationToken token, String username) {
        if (token != null) {
            MidPointPrincipal mpPrincipal = token.getPrincipal() instanceof MidPointPrincipal ? (MidPointPrincipal) token.getPrincipal() : null;
            boolean isAuthenticated = token.isAuthenticated();
            if (isAuthenticated) {
                authenticationRecorder.recordSequenceAuthenticationSuccess(mpPrincipal, connEnv);
            }
        } else {
            authenticationRecorder.recordSequenceAuthenticationFailure(username, null, null,
                    "invalid_token", connEnv);
        }
    }
}
