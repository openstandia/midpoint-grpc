import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import jp.openstandia.midpoint.grpc.*;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

public class TestModifyClient {

    public static void main(String[] args) throws UnsupportedEncodingException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 6565)
                .usePlaintext()
                .build();

        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = SelfServiceResourceGrpc.newBlockingStub(channel);

        String token = Base64.getEncoder().encodeToString("Administrator:5ecr3t".getBytes("UTF-8"));

        Metadata headers = new Metadata();
        headers.put(Constant.AuthorizationMetadataKey, "Basic " + token);
//        headers.put(Constant.SwitchToPrincipalByNameMetadataKey, "test");

        stub = MetadataUtils.attachHeaders(stub, headers);

        ModifyProfileRequest request = ModifyProfileRequest.newBuilder()
                .addModifications(
                        UserItemDelta.newBuilder()
//                                .setUserTypePath(DefaultUserTypePath.F_FAMILY_NAME)
                                .setPath("familyName")
                                .setValuesToAdd("hoge1")
                )
                .addModifications(
                        UserItemDelta.newBuilder()
//                                .setUserTypePath(DefaultUserTypePath.F_FAMILY_NAME)
                                .setPath("extension/singleString")
//                                .setPath("familyName")
//                                .setItemPath(
//                                        ItemPathMessage.newBuilder()
//                                                .addPath(QNameMessage.newBuilder().setLocalPart("extension"))
//                                                .addPath(QNameMessage.newBuilder().setLocalPart("singleString"))
//                                )
                                .setValuesToAdd("hoge2")
                )
                .build();

        ModifyProfileResponse response = stub.modifyProfile(request);

        System.out.println(response);

    }
}
