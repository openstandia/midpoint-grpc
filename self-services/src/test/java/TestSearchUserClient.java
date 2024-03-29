import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import jp.openstandia.midpoint.grpc.*;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

public class TestSearchUserClient {

    public static void main(String[] args) throws UnsupportedEncodingException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 6565)
                .usePlaintext()
                .build();

        SelfServiceResourceGrpc.SelfServiceResourceBlockingStub stub = SelfServiceResourceGrpc.newBlockingStub(channel);

        String token = Base64.getEncoder().encodeToString("Administrator:5ecr3t".getBytes("UTF-8"));

        Metadata headers = new Metadata();
        headers.put(Constant.AuthorizationMetadataKey, "Basic " + token);

        stub = MetadataUtils.attachHeaders(stub, headers);

        SearchRequest req = SearchRequest.newBuilder()
//                .addInclude("activation")
                .setQuery(
                        QueryMessage.newBuilder()
                                .setFilter(
                                        ObjectFilterMessage.newBuilder()
//                                                .setRef(
//                                                        FilterReferenceMessage.newBuilder()
//                                                                .setFullPath("assignment/targetRef")
//                                                                .setValue(
//                                                                        ReferenceMessage.newBuilder()
//                                                                                .setObjectType(DefaultObjectType.ROLE_TYPE)
//                                                                                .setOid("00000000-0000-0000-0000-000000000004")
//                                                                )
//                                                )
//                                                .setEqPolyString(
//                                                        FilterEntryMessage.newBuilder()
//                                                        .setFullPath("name")
//                                                        .setValue("test")
//                                                )
//                                                .setNot(
//                                                        NotFilterMessage.newBuilder()
//                                                                .setFilter(
//                                                                        ObjectFilterMessage.newBuilder()
//                                                                                .setContains(
//                                                                                        FilterEntryMessage.newBuilder()
//                                                                                                .setFullPath("name")
//                                                                                                .setValue("foo")
//                                                                                )
//                                                                )
//                                                )
//                                                .setContains(
//                                                        FilterEntryMessage.newBuilder()
//                                                                .setFullPath("extension/singleString")
//                                                                .setValue("foobar")
//                                                )
                                )
                                .setPaging(
                                        PagingMessage.newBuilder()
                                                .addOrdering(ObjectOrderingMessage.newBuilder()
                                                        .setOrderBy("fullName")
                                                        .setOrderDirection(OrderDirectionType.DESCENDING)
                                                )
                                )
                )
                .build();

        SearchUsersResponse res = stub.searchUsers(req);

        System.out.println(res);
    }
}
