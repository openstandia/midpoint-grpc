package jp.openstandia.midpoint.grpc;

import com.evolveum.midpoint.schema.constants.SchemaConstants;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MidPointGrpcTestRunner.class)
class SelfServiceResourceITest {
    private static ManagedChannel channel;
    private static SelfServiceResourceGrpc.SelfServiceResourceBlockingStub defaultUserStub;
    private static SelfServiceResourceGrpc.SelfServiceResourceBlockingStub defaultServiceAccountStub;

    private static final String GRPC_SERVICE_ACCOUNT_NAME = "grpc-service-account";
    private static final String GRPC_SERVICE_ROLE_NAME = "grpc-service-role";
    private static String GRPC_SERVICE_ROLE_OID = null;
    private static final String DEFAULT_TEST_USERNAME = "defaultUser001";

    @BeforeAll
    static void init() {
        channel = ManagedChannelBuilder.forAddress("localhost", 6565)
                .usePlaintext()
                .build();

        // Add gRPC service account and role
        addGrpcServiceAccount(GRPC_SERVICE_ACCOUNT_NAME, "password");
        defaultUserStub = newStubWithSwitchUserByUsername(GRPC_SERVICE_ACCOUNT_NAME, "password", DEFAULT_TEST_USERNAME, true);
        defaultServiceAccountStub = newStub(GRPC_SERVICE_ACCOUNT_NAME, "password", true);
    }

    @AfterAll
    static void cleanup() {
        // Cleanup
        // Delete gRPC service account and role
        deleteObject(DefaultObjectType.USER_TYPE, GRPC_SERVICE_ACCOUNT_NAME);
        deleteObject(DefaultObjectType.ROLE_TYPE, GRPC_SERVICE_ROLE_NAME);

        channel.shutdownNow();
    }

    @BeforeEach
    void beforeMethod() {
        // Add default test user
        addUser(DEFAULT_TEST_USERNAME, "DefaultUser001",
                "00000000-0000-0000-0000-000000000008" // End User
        );
    }

    @AfterEach
    void afterMethod() {
        // Cleanup
        // Delete default test user
        deleteObject(DefaultObjectType.USER_TYPE, DEFAULT_TEST_USERNAME);
    }

    @Test
    void unauthenticated() throws Exception {
        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = SelfServiceResourceGrpc.newBlockingStub(channel);

        String token = Base64.getEncoder().encodeToString("Administrator:invalid".getBytes("UTF-8"));

        Metadata headers = new Metadata();
        headers.put(Constant.AuthorizationMetadataKey, "Basic " + token);

        stub = MetadataUtils.attachHeaders(stub, headers);

        GetSelfRequest request = GetSelfRequest.newBuilder()
                .build();

        try {
            GetSelfResponse response = stub.getSelf(request);
            fail("No unauthenticated error");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.UNAUTHENTICATED, e.getStatus().getCode());
            assertEquals("invalid_token", e.getStatus().getDescription());
        }
    }

    @Test
    void switchUserWithDuplicateName() throws Exception {
        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = SelfServiceResourceGrpc.newBlockingStub(channel);

        String token = Base64.getEncoder().encodeToString("Administrator:5ecr3t".getBytes("UTF-8"));

        Metadata headers = new Metadata();
        headers.put(Constant.AuthorizationMetadataKey, "Basic " + token);
        headers.put(Constant.SwitchToPrincipalByNameMetadataKey, "Administrator");

        stub = MetadataUtils.attachHeaders(stub, headers);

        // Add org with same user's name
        AddOrgRequest addRequest = AddOrgRequest.newBuilder()
                .setObject(OrgTypeMessage.newBuilder()
                        .setName(PolyStringMessage.newBuilder().setOrig("Administrator"))
                )
                .build();

        AddObjectResponse addResponse = stub.addOrg(addRequest);
        assertNotNull(addResponse.getOid());

        // Test authentication again
        GetSelfRequest request = GetSelfRequest.newBuilder()
                .build();

        GetSelfResponse response = stub.getSelf(request);
        UserTypeMessage user = response.getProfile();

        assertEquals("Administrator", user.getFamilyName().getOrig());
        assertEquals("administrator", user.getFamilyName().getNorm());
    }

    @Test
    void switchUser() throws Exception {
        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = SelfServiceResourceGrpc.newBlockingStub(channel);

        String token = Base64.getEncoder().encodeToString("Administrator:5ecr3t".getBytes("UTF-8"));

        Metadata headers = new Metadata();
        headers.put(Constant.AuthorizationMetadataKey, "Basic " + token);
        headers.put(Constant.SwitchToPrincipalByNameMetadataKey, "Administrator");

        stub = MetadataUtils.attachHeaders(stub, headers);

        GetSelfRequest request = GetSelfRequest.newBuilder()
                .build();

        GetSelfResponse response = stub.getSelf(request);
        UserTypeMessage user = response.getProfile();

        assertEquals("Administrator", user.getFamilyName().getOrig());
        assertEquals("administrator", user.getFamilyName().getNorm());
    }

    @Test
    void switchUserWithNotFoundUser() throws Exception {
        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = SelfServiceResourceGrpc.newBlockingStub(channel);

        String token = Base64.getEncoder().encodeToString("Administrator:5ecr3t".getBytes("UTF-8"));

        Metadata headers = new Metadata();
        headers.put(Constant.AuthorizationMetadataKey, "Basic " + token);
        headers.put(Constant.SwitchToPrincipalByNameMetadataKey, "foo");

        stub = MetadataUtils.attachHeaders(stub, headers);

        GetSelfRequest request = GetSelfRequest.newBuilder()
                .build();

        try {
            GetSelfResponse response = stub.getSelf(request);
            fail("No unauthenticated error");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.UNAUTHENTICATED, e.getStatus().getCode());
            assertEquals("Not found user", e.getStatus().getDescription());
        }
    }

    @Test
    void switchUserWithRunPrivileged() throws Exception {
        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = SelfServiceResourceGrpc.newBlockingStub(channel);

        String token = Base64.getEncoder().encodeToString("Administrator:5ecr3t".getBytes("UTF-8"));

        Metadata headers = new Metadata();
        headers.put(Constant.AuthorizationMetadataKey, "Basic " + token);
        headers.put(Constant.SwitchToPrincipalByNameMetadataKey, "Administrator");
        headers.put(Constant.RunPrivilegedMetadataKey, "true");

        stub = MetadataUtils.attachHeaders(stub, headers);

        GetSelfRequest request = GetSelfRequest.newBuilder()
                .build();

        GetSelfResponse response = stub.getSelf(request);
        UserTypeMessage user = response.getProfile();

        assertEquals("Administrator", user.getFamilyName().getOrig());
        assertEquals("administrator", user.getFamilyName().getNorm());
    }

    @Test
    void switchUserWithUnAuthorizedUser() throws Exception {
        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = SelfServiceResourceGrpc.newBlockingStub(channel);

        String token = Base64.getEncoder().encodeToString("Administrator:5ecr3t".getBytes("UTF-8"));

        Metadata headers = new Metadata();
        headers.put(Constant.AuthorizationMetadataKey, "Basic " + token);

        stub = MetadataUtils.attachHeaders(stub, headers);

        // Add a user for switch
        AddUserRequest addRequest = AddUserRequest.newBuilder()
                .setProfile(UserTypeMessage.newBuilder()
                        .setName(PolyStringMessage.newBuilder().setOrig("switch001"))
                )
                .build();

        AddUserResponse addResponse = stub.addUser(addRequest);
        assertNotNull(addResponse.getOid());

        // Test switch user
        headers.put(Constant.AuthorizationMetadataKey, "Basic " + token);
        headers.put(Constant.SwitchToPrincipalByNameMetadataKey, "switch001");

        GetSelfRequest request = GetSelfRequest.newBuilder()
                .build();

        try {
            GetSelfResponse response = stub.getSelf(request);
            fail("No unauthenticated error");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.PERMISSION_DENIED, e.getStatus().getCode());
            assertEquals("Not authorized", e.getStatus().getDescription());
        }

        // Test switch user with run privileged
        headers.put(Constant.AuthorizationMetadataKey, "Basic " + token);
        headers.put(Constant.SwitchToPrincipalByNameMetadataKey, "switch001");
        headers.put(Constant.RunPrivilegedMetadataKey, "true");

        request = GetSelfRequest.newBuilder()
                .build();

        GetSelfResponse response = stub.getSelf(request);
        UserTypeMessage user = response.getProfile();

        assertEquals("switch001", user.getName().getOrig());
    }

    @Test
    void switchUserWithArchivedUser() throws Exception {
        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = SelfServiceResourceGrpc.newBlockingStub(channel);

        String token = Base64.getEncoder().encodeToString("Administrator:5ecr3t".getBytes("UTF-8"));

        Metadata headers = new Metadata();
        headers.put(Constant.AuthorizationMetadataKey, "Basic " + token);

        stub = MetadataUtils.attachHeaders(stub, headers);

        // Add a user for switch
        AddUserRequest addRequest = AddUserRequest.newBuilder()
                .setProfile(UserTypeMessage.newBuilder()
                        .setName(PolyStringMessage.newBuilder().setOrig("switch002"))
                        .setLifecycleState("archived")
                )
                .build();

        AddUserResponse addResponse = stub.addUser(addRequest);
        assertNotNull(addResponse.getOid());

        // Test switch user
        headers.put(Constant.AuthorizationMetadataKey, "Basic " + token);
        headers.put(Constant.SwitchToPrincipalByNameMetadataKey, "switch002");
        headers.put(Constant.RunPrivilegedMetadataKey, "true");

        GetSelfRequest request = GetSelfRequest.newBuilder()
                .build();

        // Currently, the authentication isn't rejected even if the switch user is archived
        GetSelfResponse response = stub.getSelf(request);
        UserTypeMessage user = response.getProfile();

        assertEquals("switch002", user.getName().getOrig());
    }

    @Test
    void getSelf() throws Exception {
        GetSelfRequest request = GetSelfRequest.newBuilder()
                .build();

        GetSelfResponse response = defaultUserStub.getSelf(request);
        UserTypeMessage user = response.getProfile();

        assertEquals("DefaultUser001", user.getFamilyName().getOrig());
        assertEquals("defaultuser001", user.getFamilyName().getNorm());
    }

    @Test
    void modifyProfile() throws Exception {
        ModifyProfileRequest request = ModifyProfileRequest.newBuilder()
                .addModifications(
                        UserItemDeltaMessage.newBuilder()
                                .setUserTypePath(DefaultUserTypePath.F_ADDITIONAL_NAME)
                                .addValuesToReplace("Foo")
                                .build()
                )
                .build();

        defaultUserStub.modifyProfile(request);

        assertEquals("Foo", defaultUserStub.getSelf(GetSelfRequest.newBuilder().build()).getProfile().getAdditionalName().getOrig());
    }

    @Test
    void updateCredential() throws Exception {
        UpdateCredentialRequest request = UpdateCredentialRequest.newBuilder()
                .setOld("password")
                .setNew("newpassword")
                .build();

        defaultUserStub.updateCredential(request);

        // Back to original password
        request = UpdateCredentialRequest.newBuilder()
                .setOld("newpassword")
                .setNew("password")
                .build();

        defaultUserStub.updateCredential(request);
    }

    @Test
    void passwordPolicyErrorWithSingleError() throws Exception {
        UpdateCredentialRequest request = UpdateCredentialRequest.newBuilder()
                .setOld("password")
                .setNew("123")
                .build();

        try {
            defaultUserStub.updateCredential(request);
            fail("Should be thrown Exception of password policy error");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());

            PolicyError policyError = e.getTrailers().get(SelfServiceResource.PolicyErrorMetadataKey);

            Message error = policyError.getMessage();
            assertTrue(error.hasSingle());

            SingleMessage msg = error.getSingle();
            String errKey = msg.getKey();
            assertEquals("PolicyViolationException.message.credentials.password", errKey);

            List<Message> argsList = msg.getArgsList();
            assertEquals(1, argsList.size());

            Message argsWrapper = argsList.get(0);
            assertTrue(argsWrapper.hasSingle());

            SingleMessage subMsgArgs = argsWrapper.getSingle();
            String subErrKey = subMsgArgs.getKey();
            assertEquals("ValuePolicy.minimalSizeNotMet", subErrKey);

            List<Message> subMsgArgsList = subMsgArgs.getArgsList();
            assertEquals(2, subMsgArgsList.size());

            Message subMsgArg1 = subMsgArgsList.get(0);
            assertFalse(subMsgArg1.hasSingle());
            assertFalse(subMsgArg1.hasList());
            assertEquals("5", subMsgArg1.getString());

            Message subMsgArg2 = subMsgArgsList.get(1);
            assertFalse(subMsgArg2.hasSingle());
            assertFalse(subMsgArg2.hasList());
            assertEquals("3", subMsgArg2.getString());
        }
    }

    @Test
    void passwordPolicyErrorWithMultipleError() throws Exception {
        UpdateCredentialRequest request = UpdateCredentialRequest.newBuilder()
                .setOld("password")
                .setNew("1111")
                .build();

        try {
            defaultUserStub.updateCredential(request);
            fail("Should be thrown Exception of password policy error");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());

            PolicyError policyError = e.getTrailers().get(SelfServiceResource.PolicyErrorMetadataKey);

            Message message = policyError.getMessage();
            assertTrue(message.hasSingle());

            SingleMessage msg = message.getSingle();
            String errKey = msg.getKey();
            assertEquals("PolicyViolationException.message.credentials.password", errKey);

            List<Message> argsList = msg.getArgsList();
            assertEquals(1, argsList.size());

            Message argsWrapper = argsList.get(0);
            assertFalse(argsWrapper.hasSingle(), "Should have multiple errors");
            assertTrue(argsWrapper.hasList(), "Should have multiple errors");

            MessageList msgListArg = argsWrapper.getList();
            List<Message> subMsgList = msgListArg.getMessageList();
            assertEquals(2, subMsgList.size(), "Should have multiple errors");

            Message argsWrapper1 = subMsgList.get(0);
            assertTrue(argsWrapper1.hasSingle());

            SingleMessage subMsg1Args = argsWrapper1.getSingle();
            String subErr1Key = subMsg1Args.getKey();
            assertEquals("ValuePolicy.minimalSizeNotMet", subErr1Key);

            Message argsWrapper2 = subMsgList.get(1);
            assertTrue(argsWrapper2.hasSingle());

            SingleMessage subMsg2Args = argsWrapper2.getSingle();
            String subErr2Key = subMsg2Args.getKey();
            assertEquals("ValuePolicy.minimalUniqueCharactersNotMet", subErr2Key);
        }
    }

    @Test
    void forceUpdateCredential() throws Exception {
        // Force update password
        ForceUpdateCredentialRequest request = ForceUpdateCredentialRequest.newBuilder()
                .setNew("newpassword")
                .build();

        defaultUserStub.forceUpdateCredential(request);

        // Check the password was changed
        try {
            UpdateCredentialRequest updateCredentialRequest = UpdateCredentialRequest.newBuilder()
                    .setOld("password")
                    .setNew("foobar")
                    .build();
            defaultUserStub.updateCredential(updateCredentialRequest);
            fail("Password wasn't changed");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
            assertEquals("invalid_credential", e.getStatus().getDescription());
        }

        // Force update password again
        request = ForceUpdateCredentialRequest.newBuilder()
                .setNew("newpassword2")
                .build();

        defaultUserStub.forceUpdateCredential(request);

        // Check the password was changed
        UpdateCredentialRequest updateCredentialRequest = UpdateCredentialRequest.newBuilder()
                .setOld("newpassword2")
                .setNew("password")
                .build();
        defaultUserStub.updateCredential(updateCredentialRequest);
    }

    @Test
    void forceUpdateCredentialWithNonceClear() throws Exception {
        // Save nonce to the user
        ModifyProfileRequest modifyProfileRequest = ModifyProfileRequest.newBuilder()
                .addModifications(
                        UserItemDeltaMessage.newBuilder()
                                .setPath("credentials/nonce/value")
                                .addValuesToReplace("123456")
                                .build()
                )
                .build();

        defaultUserStub.modifyProfile(modifyProfileRequest);

        // Force update password
        ForceUpdateCredentialRequest request = ForceUpdateCredentialRequest.newBuilder()
                .setNew("newpassword")
                .build();

        defaultUserStub.forceUpdateCredential(request);

        // Check the password was changed
        try {
            UpdateCredentialRequest updateCredentialRequest = UpdateCredentialRequest.newBuilder()
                    .setOld("password")
                    .setNew("newpassword")
                    .build();
            defaultUserStub.updateCredential(updateCredentialRequest);
            fail("Password wasn't changed");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
            assertEquals("invalid_credential", e.getStatus().getDescription());
        }

        // Check nonce isn't cleared yet
        CheckNonceRequest checkNonceRequest = CheckNonceRequest.newBuilder()
                .setNonce("123456")
                .build();

        CheckNonceResponse checkNonceResponse = defaultUserStub.checkNonce(checkNonceRequest);

        System.out.println(checkNonceResponse);

        assertTrue(checkNonceResponse.getValid());
        assertTrue(checkNonceResponse.getError().isEmpty());

        // Back to original password with clear nonce and active
        ForceUpdateCredentialRequest forceReq = ForceUpdateCredentialRequest.newBuilder()
                .setNew("password")
                .setClearNonce(true)
                .build();

        defaultUserStub.forceUpdateCredential(forceReq);

        // Check the password was changed
        try {
            UpdateCredentialRequest updateCredentialRequest = UpdateCredentialRequest.newBuilder()
                    .setOld("newpassword")
                    .setNew("password")
                    .build();
            defaultUserStub.updateCredential(updateCredentialRequest);
            fail("Password wasn't changed");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
            assertEquals("invalid_credential", e.getStatus().getDescription());
        }

        // Check nonce was cleared
        checkNonceResponse = defaultUserStub.checkNonce(checkNonceRequest);

        System.out.println(checkNonceResponse);

        assertFalse(checkNonceResponse.getValid());
        assertEquals("not_found", checkNonceResponse.getError());
    }

    @Test
    void forceUpdateCredentialWithNonceClearForNoCredentialsUser() throws Exception {
        // Add test user without credentials
        AddUserRequest addUserRequest = AddUserRequest.newBuilder()
                .setProfile(UserTypeMessage.newBuilder()
                        .setName(PolyStringMessage.newBuilder().setOrig("user001_no_cred"))
                )
                .build();

        AddUserResponse response = defaultUserStub.addUser(addUserRequest);

        assertNotNull(response.getOid());

        // Switch to the created user
        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = newDefaultStubWithSwitchUserByOid("user001_no_cred", true);

        // Force update password with nonce clear
        ForceUpdateCredentialRequest request = ForceUpdateCredentialRequest.newBuilder()
                .setNew("password")
                .setClearNonce(true)
                .build();

        stub.forceUpdateCredential(request);

        // Cleanup

        // Delete the test user
        defaultUserStub.deleteObject(DeleteObjectRequest.newBuilder()
                .setOid(response.getOid())
                .setObjectType(DefaultObjectType.USER_TYPE)
                .build());
    }

    @Test
    void forceUpdateCredentialWithNonceClearAndActiveByServiceAccount() throws Exception {
        // Save nonce to the default user
        ModifyUserRequest modifyUserRequest = ModifyUserRequest.newBuilder()
                .setName(DEFAULT_TEST_USERNAME)
                .addModifications(
                        UserItemDeltaMessage.newBuilder()
                                .setPath("credentials/nonce/value")
                                .addValuesToReplace("123456")
                                .build()
                )
                .build();

        defaultUserStub.modifyUser(modifyUserRequest);

        // Check nonce was saved
        CheckNonceRequest checkNonceRequest = CheckNonceRequest.newBuilder()
                .setNonce("123456")
                .build();

        CheckNonceResponse checkNonceResponse = defaultUserStub.checkNonce(checkNonceRequest);

        System.out.println(checkNonceResponse);

        assertTrue(checkNonceResponse.getValid());
        assertTrue(checkNonceResponse.getError().isEmpty());

        // Check lifecycleState isn't active yet
        assertEquals("", defaultUserStub.getSelf(GetSelfRequest.newBuilder().build()).getProfile().getLifecycleState());

        // Force update password with nonce clear and activation
        ForceUpdateCredentialRequest forceReq = ForceUpdateCredentialRequest.newBuilder()
                .setNew("newpassword")
                .setClearNonce(true)
                .setActive(true)
                .build();

        defaultUserStub.forceUpdateCredential(forceReq);

        // Check the password was changed
        try {
            UpdateCredentialRequest updateCredentialRequest = UpdateCredentialRequest.newBuilder()
                    .setOld("password")
                    .setNew("newpassword")
                    .build();
            defaultUserStub.updateCredential(updateCredentialRequest);
            fail("Password wasn't changed");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
            assertEquals("invalid_credential", e.getStatus().getDescription());
        }

        // Check nonce was cleared
        checkNonceResponse = defaultUserStub.checkNonce(checkNonceRequest);

        System.out.println(checkNonceResponse);

        assertFalse(checkNonceResponse.getValid());
        assertEquals("not_found", checkNonceResponse.getError());

        // Check lifecycleState was active
        assertEquals(SchemaConstants.LIFECYCLE_ACTIVE, defaultUserStub.getSelf(GetSelfRequest.newBuilder().build()).getProfile().getLifecycleState());
    }

    @Test
    void generateNonce() throws Exception {
        GenerateValueRequest request = GenerateValueRequest.newBuilder()
                .setValuePolicyOid("00000000-0000-0000-0000-000000000003")
                .build();

        GenerateValueResponse response = defaultServiceAccountStub.generateValue(request);

        System.out.println("Generated nonce: " + response.getValue());

        assertNotNull(response.getValue());
        assertTrue(response.getValue().length() > 0);
    }

    @Test
    void checkNonce() throws Exception {
        // Save lifecycle to the default user
        ModifyUserRequest modifyUserRequest = ModifyUserRequest.newBuilder()
                .setName(DEFAULT_TEST_USERNAME)
                .addModifications(
                        UserItemDeltaMessage.newBuilder()
                                .setPath("lifecycleState")
                                .addValuesToReplace("proposed")
                                .build()
                )
                .build();

        defaultUserStub.modifyUser(modifyUserRequest);

        // Check nonce when the user doesn't have
        CheckNonceRequest checkNonceRequest = CheckNonceRequest.newBuilder()
                .setNonce("123456")
                .build();

        CheckNonceResponse checkNonceResponse = defaultUserStub.checkNonce(checkNonceRequest);

        System.out.println(checkNonceResponse);

        assertFalse(checkNonceResponse.getValid());
        assertEquals("not_found", checkNonceResponse.getError());

        // Save nonce to the user
        ModifyProfileRequest modifyProfileRequest = ModifyProfileRequest.newBuilder()
                .addModifications(
                        UserItemDeltaMessage.newBuilder()
                                .setPath("credentials/nonce/value")
                                .addValuesToReplace("123456")
                                .build()
                )
                .build();

        defaultUserStub.modifyProfile(modifyProfileRequest);

        // Check nonce again
        checkNonceResponse = defaultUserStub.checkNonce(checkNonceRequest);

        System.out.println(checkNonceResponse);

        assertTrue(checkNonceResponse.getValid());
        assertTrue(checkNonceResponse.getError().isEmpty());

        // Check nonce with invalid value
        CheckNonceRequest invalidCheckNonceRequest = CheckNonceRequest.newBuilder()
                .setNonce("invalid")
                .build();
        checkNonceResponse = defaultUserStub.checkNonce(invalidCheckNonceRequest);

        System.out.println(checkNonceResponse);

        assertFalse(checkNonceResponse.getValid());
        assertEquals("invalid", checkNonceResponse.getError());

        // Update credential
        ForceUpdateCredentialRequest forceUpdateCredentialRequest = ForceUpdateCredentialRequest.newBuilder()
                .setNew("newpassword")
                .build();

        defaultUserStub.forceUpdateCredential(forceUpdateCredentialRequest);

        // Check nonce isn't cleared yet
        checkNonceResponse = defaultUserStub.checkNonce(checkNonceRequest);

        System.out.println(checkNonceResponse);

        assertTrue(checkNonceResponse.getValid());
        assertTrue(checkNonceResponse.getError().isEmpty());

        // Update credential again
        UpdateCredentialRequest updateCredentialRequest = UpdateCredentialRequest.newBuilder()
                .setOld("newpassword")
                .setNew("newpassword2")
                .build();

        defaultUserStub.updateCredential(updateCredentialRequest);

        // Check nonce isn't cleared yet
        checkNonceResponse = defaultUserStub.checkNonce(checkNonceRequest);

        System.out.println(checkNonceResponse);

        assertTrue(checkNonceResponse.getValid());
        assertTrue(checkNonceResponse.getError().isEmpty());

        // Check lifecycleState isn't active yet
        assertEquals(SchemaConstants.LIFECYCLE_PROPOSED, defaultUserStub.getSelf(GetSelfRequest.newBuilder().build()).getProfile().getLifecycleState());

        // Update credential again with nonce clearing and activation
        ForceUpdateCredentialRequest forceUpdateCredentialRequestWithClearNonce = ForceUpdateCredentialRequest.newBuilder()
                .setNew("password")
                .setClearNonce(true)
                .setActive(true)
                .build();

        defaultUserStub.forceUpdateCredential(forceUpdateCredentialRequestWithClearNonce);

        // Check nonce was cleared
        checkNonceResponse = defaultUserStub.checkNonce(checkNonceRequest);

        System.out.println(checkNonceResponse);

        assertFalse(checkNonceResponse.getValid());
        assertEquals("not_found", checkNonceResponse.getError());

        // Check lifecycleState was active
        assertEquals(SchemaConstants.LIFECYCLE_ACTIVE, defaultUserStub.getSelf(GetSelfRequest.newBuilder().build()).getProfile().getLifecycleState());
    }

    @Test
    void requestRole() {
    }

    @Test
    void user() throws Exception {
        // Add
        AddUserRequest request = AddUserRequest.newBuilder()
                .setProfile(UserTypeMessage.newBuilder()
                        .setName(PolyStringMessage.newBuilder().setOrig("user001"))
                        .setEmployeeNumber("emp001")
                )
                .build();

        AddUserResponse response = defaultServiceAccountStub.addUser(request);

        assertNotNull(response.getOid());

        // Get
        GetUserRequest req2 = GetUserRequest.newBuilder()
                .setOid(response.getOid())
                .build();

        GetUserResponse res2 = defaultServiceAccountStub.getUser(req2);

        assertEquals("user001", res2.getResult().getName().getOrig());
        assertEquals("emp001", res2.getResult().getEmployeeNumber());

        // Modify by name
        ModifyUserRequest modReq = ModifyUserRequest.newBuilder()
                .setName("user001")
                .addModifications(UserItemDeltaMessage.newBuilder()
                        .setUserTypePath(DefaultUserTypePath.F_NAME)
                        .addValuesToReplace("user001_mod")
                )
                .build();

        defaultServiceAccountStub.modifyUser(modReq);

        // Get by name
        req2 = GetUserRequest.newBuilder()
                .setName("user001_mod")
                .build();

        res2 = defaultServiceAccountStub.getUser(req2);

        assertEquals("emp001", res2.getResult().getEmployeeNumber());

        // Search
        SearchUsersResponse res3 = defaultServiceAccountStub.searchUsers(SearchRequest.newBuilder()
                .setQuery(QueryMessage.newBuilder()
                        .setFilter(ObjectFilterMessage.newBuilder()
                                .setEq(FilterEntryMessage.newBuilder()
                                        .setFullPath("employeeNumber")
                                        .setValue("emp001"))))
                .build());

        assertEquals(1, res3.getNumberOfAllResults());
        assertEquals("emp001", res3.getResults(0).getEmployeeNumber());

        // Cleanup

        // Delete
        DeleteObjectResponse res4 = defaultServiceAccountStub.deleteObject(DeleteObjectRequest.newBuilder()
                .setOid(response.getOid())
                .setObjectType(DefaultObjectType.USER_TYPE)
                .build());

        assertNotNull(res4);
    }

    @Test
    void role() throws Exception {
        // Add
        AddRoleRequest request = AddRoleRequest.newBuilder()
                .setObject(RoleTypeMessage.newBuilder()
                        .setName(PolyStringMessage.newBuilder().setOrig("role001"))
                        .addSubtype("testRole")
                )
                .build();

        AddObjectResponse response = defaultServiceAccountStub.addRole(request);

        assertNotNull(response.getOid());

        // Get
        GetRoleRequest req2 = GetRoleRequest.newBuilder()
                .setOid(response.getOid())
                .build();

        GetRoleResponse res2 = defaultServiceAccountStub.getRole(req2);

        assertEquals("role001", res2.getResult().getName().getOrig());
        assertEquals("testRole", res2.getResult().getSubtype(0));

        // Modify by name
        ModifyObjectRequest modReq = ModifyObjectRequest.newBuilder()
                .setObjectType(DefaultObjectType.ROLE_TYPE)
                .setName("role001")
                .addModifications(ItemDeltaMessage.newBuilder()
                        .setPath("name")
                        .addPrismValuesToReplace(PrismValueMessage.newBuilder()
                                .setProperty(PrismPropertyValueMessage.newBuilder()
                                        .setPolyString(PolyStringMessage.newBuilder()
                                                .setOrig("role001_mod")
                                        )
                                )
                        )
                )
                .build();

        defaultServiceAccountStub.modifyObject(modReq);

        // Get by name
        req2 = GetRoleRequest.newBuilder()
                .setName("role001_mod")
                .build();

        res2 = defaultServiceAccountStub.getRole(req2);

        assertEquals("role001_mod", res2.getResult().getName().getOrig());

        // Search
        SearchRolesResponse res3 = defaultServiceAccountStub.searchRoles(SearchRequest.newBuilder()
                .setQuery(QueryMessage.newBuilder()
                        .setFilter(ObjectFilterMessage.newBuilder()
                                .setEq(FilterEntryMessage.newBuilder()
                                        .setFullPath("subtype")
                                        .setValue("testRole"))))
                .build());

        assertEquals(1, res3.getNumberOfAllResults());
        assertEquals("testRole", res3.getResults(0).getSubtype(0));

        // Delete
        DeleteObjectResponse res4 = defaultServiceAccountStub.deleteObject(DeleteObjectRequest.newBuilder()
                .setOid(response.getOid())
                .setObjectType(DefaultObjectType.ROLE_TYPE)
                .build());

        assertNotNull(res4);
    }

    @Test
    void org() throws Exception {
        // Add
        AddOrgRequest request = AddOrgRequest.newBuilder()
                .setObject(OrgTypeMessage.newBuilder()
                        .setName(PolyStringMessage.newBuilder().setOrig("org001"))
                        .addSubtype("testOrg")
                        .setDisplayOrder(1)
                )
                .build();

        AddObjectResponse response = defaultServiceAccountStub.addOrg(request);

        assertNotNull(response.getOid());

        // Get
        GetOrgRequest req2 = GetOrgRequest.newBuilder()
                .setOid(response.getOid())
                .build();

        GetOrgResponse res2 = defaultServiceAccountStub.getOrg(req2);

        assertEquals("org001", res2.getResult().getName().getOrig());
        assertEquals(1, res2.getResult().getDisplayOrder());

        // Modify by name
        ModifyObjectRequest modReq = ModifyObjectRequest.newBuilder()
                .setObjectType(DefaultObjectType.ORG_TYPE)
                .setName("org001")
                .addModifications(ItemDeltaMessage.newBuilder()
                        .setPath("name")
                        .addPrismValuesToReplace(PrismValueMessage.newBuilder()
                                .setProperty(PrismPropertyValueMessage.newBuilder()
                                        .setPolyString(PolyStringMessage.newBuilder()
                                                .setOrig("org001_mod")
                                        )
                                )
                        )
                )
                .build();

        defaultServiceAccountStub.modifyObject(modReq);

        // Get by name
        req2 = GetOrgRequest.newBuilder()
                .setName("org001_mod")
                .build();

        res2 = defaultServiceAccountStub.getOrg(req2);

        assertEquals("org001_mod", res2.getResult().getName().getOrig());
        assertEquals(1, res2.getResult().getDisplayOrder());

        // Search
        SearchOrgsResponse res3 = defaultServiceAccountStub.searchOrgs(SearchRequest.newBuilder()
                .setQuery(QueryMessage.newBuilder()
                        .setFilter(ObjectFilterMessage.newBuilder()
                                .setEq(FilterEntryMessage.newBuilder()
                                        .setFullPath("subtype")
                                        .setValue("testOrg"))))
                .build());

        assertEquals(1, res3.getNumberOfAllResults());
        assertEquals(1, res3.getResults(0).getDisplayOrder());

        // Delete by name
        DeleteObjectResponse res4 = defaultServiceAccountStub.deleteObject(DeleteObjectRequest.newBuilder()
                .setName("org001_mod")
                .setObjectType(DefaultObjectType.ORG_TYPE)
                .build());

        assertNotNull(res4);
    }

    @Test
    void service() throws Exception {
        // Add
        AddServiceRequest request = AddServiceRequest.newBuilder()
                .setObject(ServiceTypeMessage.newBuilder()
                        .setName(PolyStringMessage.newBuilder().setOrig("service001"))
                        .addSubtype("testService")
                        .setUrl("https://example.com")
                )
                .build();

        AddObjectResponse response = defaultServiceAccountStub.addService(request);

        assertNotNull(response.getOid());

        // Get
        GetServiceRequest req2 = GetServiceRequest.newBuilder()
                .setOid(response.getOid())
                .build();

        GetServiceResponse res2 = defaultServiceAccountStub.getService(req2);

        assertEquals("service001", res2.getResult().getName().getOrig());
        assertEquals("https://example.com", res2.getResult().getUrl());

        // Modify by name
        ModifyObjectRequest modReq = ModifyObjectRequest.newBuilder()
                .setObjectType(DefaultObjectType.SERVICE_TYPE)
                .setName("service001")
                .addModifications(ItemDeltaMessage.newBuilder()
                        .setItemPath(ItemPathMessage.newBuilder()
                                .addPath(QNameMessage.newBuilder()
                                        .setLocalPart("name")
                                )
                        )
                        .addPrismValuesToReplace(PrismValueMessage.newBuilder()
                                .setProperty(PrismPropertyValueMessage.newBuilder()
                                        .setPolyString(PolyStringMessage.newBuilder()
                                                .setOrig("service001_mod")
                                        )
                                )
                        )
                )
                .build();

        defaultServiceAccountStub.modifyObject(modReq);

        // Get by name
        req2 = GetServiceRequest.newBuilder()
                .setName("service001_mod")
                .build();

        res2 = defaultServiceAccountStub.getService(req2);

        assertEquals("service001_mod", res2.getResult().getName().getOrig());

        // Search
        SearchServicesResponse res3 = defaultServiceAccountStub.searchServices(SearchRequest.newBuilder()
                .setQuery(QueryMessage.newBuilder()
                        .setFilter(ObjectFilterMessage.newBuilder()
                                .setEq(FilterEntryMessage.newBuilder()
                                        .setFullPath("subtype")
                                        .setValue("testService"))))
                .build());

        assertEquals(1, res3.getNumberOfAllResults());
        assertEquals("https://example.com", res3.getResults(0).getUrl());

        // Delete
        DeleteObjectResponse res4 = defaultServiceAccountStub.deleteObject(DeleteObjectRequest.newBuilder()
                .setOid(response.getOid())
                .setObjectType(DefaultObjectType.SERVICE_TYPE)
                .build());

        assertNotNull(res4);
    }

    @Test
    void recompute() throws Exception {
        // Recompute user
        defaultServiceAccountStub.recomputeObject(RecomputeObjectRequest.newBuilder()
                .setObjectType(DefaultObjectType.USER_TYPE)
                .setOid("00000000-0000-0000-0000-000000000002")
                .build()
        );

        // Recompute role by name
        defaultServiceAccountStub.recomputeObject(RecomputeObjectRequest.newBuilder()
                .setObjectType(DefaultObjectType.ROLE_TYPE)
                .setName(GRPC_SERVICE_ROLE_NAME)
                .build()
        );
    }

    @Test
    void lookupTable() throws Exception {
        // Get with default options
        GetLookupTableRequest req = GetLookupTableRequest.newBuilder()
                .setName("Languages")
                .build();

        GetLookupTableResponse res = defaultServiceAccountStub.getLookupTable(req);

        assertEquals("00000000-0000-0000-0000-000000000200", res.getResult().getOid());
        assertEquals("1", res.getResult().getVersion());
        assertEquals("Languages", res.getResult().getName().getOrig());
        assertEquals("\n        Lookup table for languages directly supported by midPoint.\n        This lookup table contains language codes that are supported by\n        midPoint product localizations. The idea is that only the list of\n        supported languages will be presented to midPoint user.\n        For full list of language code please see lookup-languages-all.xml\n        in midPoint samples.\n    ",
                res.getResult().getDescription());
        assertEquals(0, res.getResult().getRowCount());

        // Get with all rows
        req = GetLookupTableRequest.newBuilder()
                .setName("Timezones")
                .addInclude("row")
                .build();

        res = defaultServiceAccountStub.getLookupTable(req);

        assertEquals("00000000-0000-0000-0000-000000000220", res.getResult().getOid());
        assertEquals("1", res.getResult().getVersion());
        assertEquals("Timezones", res.getResult().getName().getOrig());
        assertEquals("", res.getResult().getDescription());
        assertNotNull(res.getResult().getRowList());
        List<LookupTableRowMessage> rows = res.getResult().getRowList();
        assertEquals(419, rows.size());

        assertEquals("Africa/Abidjan", rows.get(0).getKey());
        assertEquals("Africa/Abidjan", rows.get(0).getLabel().getOrig());
        assertEquals("117", rows.get(0).getValue());
        assertEquals(118, rows.get(0).getId());
        assertTrue(rows.get(0).hasLastChangeTimestamp());

        // with query by key
        req = GetLookupTableRequest.newBuilder()
                .setName("Languages")
                .setRelationalValueSearchQuery(RelationalValueSearchQueryMessage.newBuilder()
                        .setColumn(QNameMessage.newBuilder()
                                .setLocalPart("key"))
                        .setSearchType(RelationalValueSearch.EXACT)
                        .setSearchValue("en")
                )
                .build();

        res = defaultServiceAccountStub.getLookupTable(req);

        assertEquals("00000000-0000-0000-0000-000000000200", res.getResult().getOid());
        assertEquals("1", res.getResult().getVersion());
        assertEquals("Languages", res.getResult().getName().getOrig());
        assertEquals("", res.getResult().getDescription());
        rows = res.getResult().getRowList();
        assertEquals(1, rows.size());

        assertEquals("en", rows.get(0).getKey());
        assertEquals("English", rows.get(0).getLabel().getOrig());
        assertEquals("", rows.get(0).getValue());
        assertEquals(4, rows.get(0).getId());
        assertTrue(rows.get(0).hasLastChangeTimestamp());

        // with query and paging
        req = GetLookupTableRequest.newBuilder()
                .setName("Languages")
                .addInclude("description")
                .setRelationalValueSearchQuery(RelationalValueSearchQueryMessage.newBuilder()
                        .setPaging(PagingMessage.newBuilder()
                                .addOrdering(ObjectOrderingMessage.newBuilder()
                                        .setOrderBy("key")
                                        .setOrderDirection(OrderDirectionType.ASCENDING)
                                )
                                .setOffset(0)
                                .setMaxSize(2)
                        )
                )
                .build();

        res = defaultServiceAccountStub.getLookupTable(req);

        assertEquals("00000000-0000-0000-0000-000000000200", res.getResult().getOid());
        assertEquals("1", res.getResult().getVersion());
        assertEquals("Languages", res.getResult().getName().getOrig());
        assertEquals("\n        Lookup table for languages directly supported by midPoint.\n        This lookup table contains language codes that are supported by\n        midPoint product localizations. The idea is that only the list of\n        supported languages will be presented to midPoint user.\n        For full list of language code please see lookup-languages-all.xml\n        in midPoint samples.\n    ",
                res.getResult().getDescription());
        rows = res.getResult().getRowList();
        assertEquals(2, rows.size());

        assertEquals("cs", rows.get(0).getKey());
        assertEquals("Čeština", rows.get(0).getLabel().getOrig());
        assertEquals("", rows.get(0).getValue());
        assertEquals(1, rows.get(0).getId());
        assertTrue(rows.get(0).hasLastChangeTimestamp());

        assertEquals("de", rows.get(1).getKey());
        assertEquals("Deutsch", rows.get(1).getLabel().getOrig());
        assertEquals("", rows.get(1).getValue());
        assertEquals(2, rows.get(1).getId());
        assertTrue(rows.get(1).hasLastChangeTimestamp());

        // add new row
        ModifyObjectRequest addRowReq = ModifyObjectRequest.newBuilder()
                .setOid("00000000-0000-0000-0000-000000000200")
                .setObjectType(DefaultObjectType.LOOKUP_TABLE_TYPE)
                .addModifications(ItemDeltaMessage.newBuilder()
                        .setPath("row")
                        .addPrismValuesToAdd(PrismValueMessage.newBuilder()
                                .setContainer(PrismContainerValueMessage.newBuilder()
                                        .putValue("key", ItemMessage.newBuilder()
                                                .setProperty(PrismPropertyMessage.newBuilder()
                                                        .addValues(PrismPropertyValueMessage.newBuilder()
                                                                .setString("newKey")
                                                        )
                                                )
                                                .build()
                                        )
                                        .putValue("label", ItemMessage.newBuilder()
                                                .setProperty(PrismPropertyMessage.newBuilder()
                                                        .addValues(PrismPropertyValueMessage.newBuilder()
                                                                .setPolyString(PolyStringMessage.newBuilder()
                                                                        .setOrig("newLabel")
                                                                )
                                                        )
                                                )
                                                .build()
                                        )
                                        .putValue("value", ItemMessage.newBuilder()
                                                .setProperty(PrismPropertyMessage.newBuilder()
                                                        .addValues(PrismPropertyValueMessage.newBuilder()
                                                                .setString("newValue")
                                                        )
                                                )
                                                .build()
                                        )
                                )
                        )
                )
                .build();

        ModifyObjectResponse addRowRes = defaultServiceAccountStub.modifyObject(addRowReq);

        req = GetLookupTableRequest.newBuilder()
                .setName("Languages")
                .setRelationalValueSearchQuery(RelationalValueSearchQueryMessage.newBuilder()
                        .setColumn(QNameMessage.newBuilder()
                                .setLocalPart("key"))
                        .setSearchType(RelationalValueSearch.EXACT)
                        .setSearchValue("newKey")
                )
                .build();

        res = defaultServiceAccountStub.getLookupTable(req);

        rows = res.getResult().getRowList();
        assertEquals(1, rows.size());

        assertEquals("newKey", rows.get(0).getKey());
        assertEquals("newLabel", rows.get(0).getLabel().getOrig());
        assertEquals("newValue", rows.get(0).getValue());
        assertTrue(rows.get(0).getId() != 0);
        assertTrue(rows.get(0).hasLastChangeTimestamp());

        // update row
        long id = rows.get(0).getId();
        ModifyObjectRequest updateRowReq = ModifyObjectRequest.newBuilder()
                .setOid("00000000-0000-0000-0000-000000000200")
                .setObjectType(DefaultObjectType.LOOKUP_TABLE_TYPE)
                .addModifications(ItemDeltaMessage.newBuilder()
                        .setPath("row/[" + id + "]/label")
                        .addPrismValuesToReplace(PrismValueMessage.newBuilder()
                                .setProperty(PrismPropertyValueMessage.newBuilder()
                                        .setPolyString(PolyStringMessage.newBuilder()
                                                .setOrig("modified")
                                        )
                                )
                        )
                )
                .addOptions("raw")
                .build();

        ModifyObjectResponse updateRowRes = defaultServiceAccountStub.modifyObject(updateRowReq);

        res = defaultServiceAccountStub.getLookupTable(req);

        rows = res.getResult().getRowList();
        assertEquals(1, rows.size());

        assertEquals("newKey", rows.get(0).getKey());
        assertEquals("modified", rows.get(0).getLabel().getOrig());
        assertEquals("newValue", rows.get(0).getValue());
        assertTrue(rows.get(0).getId() != 0);
        assertTrue(rows.get(0).hasLastChangeTimestamp());

        // delete row by id
        ModifyObjectRequest deleteRowReq = ModifyObjectRequest.newBuilder()
                .setOid("00000000-0000-0000-0000-000000000200")
                .setObjectType(DefaultObjectType.LOOKUP_TABLE_TYPE)
                .addModifications(ItemDeltaMessage.newBuilder()
                        .setPath("row")
                        .addPrismValuesToDelete(PrismValueMessage.newBuilder()
                                .setContainer(PrismContainerValueMessage.newBuilder()
                                        .setId(id)
                                )
                        )
                )
                .addOptions("raw")
                .build();

        ModifyObjectResponse deleteRowRes = defaultServiceAccountStub.modifyObject(deleteRowReq);

        res = defaultServiceAccountStub.getLookupTable(req);

        rows = res.getResult().getRowList();
        assertEquals(0, rows.size());
    }

    @Test
    void getSequenceCounter() throws Exception {
        // create new SequenceType
        AddObjectRequest newSeqReq = AddObjectRequest.newBuilder()
                .setType(QNameMessage.newBuilder().setLocalPart("SequenceType"))
                .setObject(PrismContainerMessage.newBuilder()
                        .addValues(PrismContainerValueMessage.newBuilder()
                                .putValue("name", ItemMessage.newBuilder()
                                        .setProperty(PrismPropertyMessage.newBuilder()
                                                .addValues(PrismPropertyValueMessage.newBuilder()
                                                        .setPolyString(PolyStringMessage.newBuilder()
                                                                .setOrig("Unix UID numbers")
                                                        )
                                                )
                                        ).build()
                                )
                                .putValue("counter", ItemMessage.newBuilder()
                                        .setProperty(PrismPropertyMessage.newBuilder()
                                                .addValues(PrismPropertyValueMessage.newBuilder()
                                                        .setLong(LongMessage.newBuilder()
                                                                .setValue(1001)
                                                        )
                                                )
                                        )
                                        .build()
                                )
                                .putValue("maxUnusedValues", ItemMessage.newBuilder()
                                        .setProperty(PrismPropertyMessage.newBuilder()
                                                .addValues(PrismPropertyValueMessage.newBuilder()
                                                        .setLong(LongMessage.newBuilder()
                                                                .setValue(10)
                                                        )
                                                )
                                        )
                                        .build()
                                )
                        )
                )
                .build();

        AddObjectResponse addSeqRes = defaultServiceAccountStub.addObject(newSeqReq);

        // increment
        GetSequenceCounterRequest req = GetSequenceCounterRequest.newBuilder()
                .setName("Unix UID numbers")
                .build();

        GetSequenceCounterResponse res = defaultServiceAccountStub.getSequenceCounter(req);

        assertEquals(1001, res.getResult());

        // increment
        res = defaultServiceAccountStub.getSequenceCounter(req);

        assertEquals(1002, res.getResult());
    }

    // Utilities
    private static void addGrpcServiceAccount(String username, String password) {
        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = newStubByAdministrator();

        // Add role with authorization for REST API which is required for gRPC calling
        AddObjectRequest addRoleRequest = AddObjectRequest.newBuilder()
                .setObjectType(DefaultObjectType.ROLE_TYPE)
                .setObject(PrismContainerMessage.newBuilder()
                        .addValues(PrismContainerValueMessage.newBuilder()
                                .putValue("name", ItemMessage.newBuilder()
                                        .setProperty(PrismPropertyMessage.newBuilder()
                                                .addValues(PrismPropertyValueMessage.newBuilder()
                                                        .setPolyString(PolyStringMessage.newBuilder()
                                                                .setOrig(GRPC_SERVICE_ROLE_NAME)
                                                        )
                                                )
                                        )
                                        .build()
                                )
                                .putValue("authorization", ItemMessage.newBuilder()
                                        .setContainer(PrismContainerMessage.newBuilder()
                                                .addValues(PrismContainerValueMessage.newBuilder()
                                                        .putValue("action", ItemMessage.newBuilder()
                                                                .setProperty(PrismPropertyMessage.newBuilder()
                                                                        .addValues(PrismPropertyValueMessage.newBuilder()
                                                                                .setString("http://midpoint.evolveum.com/xml/ns/public/security/authorization-rest-3#all")
                                                                        )
                                                                )
                                                                .build()
                                                        )
                                                )
                                                .addValues(PrismContainerValueMessage.newBuilder()
                                                        .putValue("action", ItemMessage.newBuilder()
                                                                .setProperty(PrismPropertyMessage.newBuilder()
                                                                        .addValues(PrismPropertyValueMessage.newBuilder()
                                                                                .setString("http://midpoint.evolveum.com/xml/ns/public/security/authorization-rest-3#proxy")
                                                                        )
                                                                )
                                                                .build()
                                                        )
                                                        .putValue("object", ItemMessage.newBuilder()
                                                                .setContainer(PrismContainerMessage.newBuilder()
                                                                        .addValues(PrismContainerValueMessage.newBuilder()
                                                                                .putValue("type", ItemMessage.newBuilder()
                                                                                        .setProperty(PrismPropertyMessage.newBuilder()
                                                                                                .addValues(PrismPropertyValueMessage.newBuilder()
                                                                                                        .setString("UserType")
                                                                                                )
                                                                                        )
                                                                                        .build()
                                                                                )
                                                                        )
                                                                )
                                                                .build()
                                                        )
                                                )
                                        )
                                        .build()
                                )
                        )
                )
                .build();
        ;
        AddObjectResponse addRoleResponse = stub.addObject(addRoleRequest);

        assertNotNull(addRoleResponse.getOid());

        GRPC_SERVICE_ROLE_OID = addRoleResponse.getOid();

        // Add user with added role assignment
        AddUserRequest addUserRequest = AddUserRequest.newBuilder()
                .setProfile(UserTypeMessage.newBuilder()
                        .setName(PolyStringMessage.newBuilder().setOrig(username))
                        .addAssignment(AssignmentMessage.newBuilder()
                                .setTargetRef(ReferenceMessage.newBuilder()
                                        .setOid(addRoleResponse.getOid())
                                        .setObjectType(DefaultObjectType.ROLE_TYPE)
                                )
                        )
                )
                .build();

        AddUserResponse response = stub.addUser(addUserRequest);

        assertNotNull(response.getOid());

        // Save password to the service account
        ModifyUserRequest modifyUserRequest = ModifyUserRequest.newBuilder()
                .setOid(response.getOid())
                .addModifications(
                        UserItemDeltaMessage.newBuilder()
                                .setPath("credentials/password/value")
                                .addValuesToReplace(password)
                                .build()
                )
                .build();

        stub.modifyUser(modifyUserRequest);
    }

    private static String addUser(String username, String familyName, String... roleOid) {
        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = newStubByAdministrator();

        UserTypeMessage.Builder builder = UserTypeMessage.newBuilder()
                .setName(PolyStringMessage.newBuilder().setOrig(username))
                .setFamilyName(PolyStringMessage.newBuilder().setOrig(familyName).build());

        // Role assignment
        Arrays.stream(roleOid).forEach(oid -> {
            builder.addAssignment(AssignmentMessage.newBuilder()
                    .setTargetRef(ReferenceMessage.newBuilder()
                            .setOid(oid)
                            .setObjectType(DefaultObjectType.ROLE_TYPE)
                    )
            );
        });

        // Add
        AddUserRequest addUserRequest = AddUserRequest.newBuilder()
                .setProfile(builder)
                .build();

        AddUserResponse response = stub.addUser(addUserRequest);

        assertNotNull(response.getOid());

        // Save default password
        ModifyUserRequest modifyUserRequest = ModifyUserRequest.newBuilder()
                .setOid(response.getOid())
                .addModifications(
                        UserItemDeltaMessage.newBuilder()
                                .setPath("credentials/password/value")
                                .addValuesToReplace("password")
                                .build()
                )
                .build();

        stub.modifyUser(modifyUserRequest);

        return response.getOid();
    }

    private static void deleteObject(DefaultObjectType type, String name) {
        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = newStubByAdministrator();
        stub.deleteObject(DeleteObjectRequest.newBuilder()
                .setName(name)
                .setObjectType(type)
                .build());
    }

    private static SelfServiceResourceGrpc.SelfServiceResourceBlockingStub newStubByAdministrator() {
        return newStub("Administrator", "5ecr3t", false);
    }

    private static SelfServiceResourceGrpc.SelfServiceResourceBlockingStub newStub(String username, String password) {
        return newStub(username, password, false);
    }

    private static SelfServiceResourceGrpc.SelfServiceResourceBlockingStub newStub(String username, String password, boolean runPrivileged) {
        return newStub(username, password, null, null, runPrivileged);
    }

    private static SelfServiceResourceGrpc.SelfServiceResourceBlockingStub newStubWithSwitchUserByOid(String username, String password, String switchUserOid, boolean runPrivileged) {
        return newStub(username, password, switchUserOid, null, runPrivileged);
    }

    private static SelfServiceResourceGrpc.SelfServiceResourceBlockingStub newStubWithSwitchUserByUsername(String username, String password, String switchUsername, boolean runPrivileged) {
        return newStub(username, password, null, switchUsername, runPrivileged);
    }

    private static SelfServiceResourceGrpc.SelfServiceResourceBlockingStub newDefaultStubWithSwitchUserByOid(String switchUsername, boolean runPrivileged) {
        return newStub(GRPC_SERVICE_ACCOUNT_NAME, "password", null, switchUsername, runPrivileged);
    }

    private static SelfServiceResourceGrpc.SelfServiceResourceBlockingStub newDefaultStubWithSwitchUserByUsername(String switchUsername, boolean runPrivileged) {
        return newStub(GRPC_SERVICE_ACCOUNT_NAME, "password", null, switchUsername, runPrivileged);
    }

    private static SelfServiceResourceGrpc.SelfServiceResourceBlockingStub newStub(String username, String password, String switchUserOid, String switchUsername, boolean runPrivileged) {
        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = SelfServiceResourceGrpc.newBlockingStub(channel);

        Metadata headers = new Metadata();

        final StringBuilder tmp = new StringBuilder();
        tmp.append(username);
        tmp.append(":");
        tmp.append(password);

        headers.put(Constant.AuthorizationMetadataKey, "Basic " +
                Base64.getEncoder().encodeToString(tmp.toString().getBytes(Charset.forName("UTF-8"))));
        if (switchUsername != null) {
            headers.put(Constant.SwitchToPrincipalByNameMetadataKey, switchUsername);
        } else if (switchUserOid != null) {
            headers.put(Constant.SwitchToPrincipalMetadataKey, switchUserOid);
        }
        if (runPrivileged) {
            headers.put(Constant.RunPrivilegedMetadataKey, "true");
        }

        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub authStub = MetadataUtils.attachHeaders(stub, headers);

        return authStub;
    }
}