package jp.openstandia.midpoint.grpc;

import com.evolveum.midpoint.prism.*;
import com.evolveum.midpoint.prism.crypto.EncryptionException;
import com.evolveum.midpoint.prism.crypto.Protector;
import com.evolveum.midpoint.prism.impl.PrismPropertyValueImpl;
import com.evolveum.midpoint.prism.impl.query.builder.QueryBuilder;
import com.evolveum.midpoint.prism.path.ItemName;
import com.evolveum.midpoint.prism.path.ItemPath;
import com.evolveum.midpoint.prism.polystring.PolyString;
import com.evolveum.midpoint.prism.query.*;
import com.evolveum.midpoint.prism.query.builder.*;
import com.evolveum.midpoint.repo.api.RepositoryService;
import com.evolveum.midpoint.schema.GetOperationOptions;
import com.evolveum.midpoint.schema.RelationalValueSearchQuery;
import com.evolveum.midpoint.schema.RelationalValueSearchType;
import com.evolveum.midpoint.schema.SelectorOptions;
import com.evolveum.midpoint.schema.constants.SchemaConstants;
import com.evolveum.midpoint.util.LocalizableMessage;
import com.evolveum.midpoint.util.LocalizableMessageList;
import com.evolveum.midpoint.util.SingleLocalizableMessage;
import com.evolveum.midpoint.util.exception.PolicyViolationException;
import com.evolveum.midpoint.util.exception.SchemaException;
import com.evolveum.midpoint.util.logging.Trace;
import com.evolveum.midpoint.util.logging.TraceManager;
import com.evolveum.midpoint.xml.ns._public.common.common_3.*;
import com.evolveum.prism.xml.ns._public.query_3.SearchFilterType;
import com.evolveum.prism.xml.ns._public.types_3.PolyStringType;
import com.evolveum.prism.xml.ns._public.types_3.ProtectedStringType;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.namespace.QName;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

public class TypeConverter {

    private static final Trace LOGGER = TraceManager.getTrace(SelfServiceResource.class);
    private static Map<DefaultUserTypePath, ItemName> userTypeMap = new HashMap<>();

    static {
        Map<String, ItemName> strToItemName = new HashMap<>();

        Field[] fields = UserType.class.getFields();
        Arrays.stream(fields)
                .filter(x -> x.getName().startsWith("F_") && x.getType() == ItemName.class)
                .forEach(x -> {
                    String name = x.getName();
                    try {
                        DefaultUserTypePath path = DefaultUserTypePath.valueOf(name);
                        ItemName itemName = (ItemName) x.get(null);
                        userTypeMap.put(path, itemName);
                        strToItemName.put(itemName.getLocalPart(), itemName);
                    } catch (IllegalArgumentException | IllegalAccessException ignore) {
                    }
                });
    }

    public static ItemName toItemName(DefaultUserTypePath path) {
        ItemName itemName = userTypeMap.get(path);
        if (itemName == null) {
            throw new UnsupportedOperationException(path + " is not supported");
        }
        return itemName;
    }

    public static List toRealValue(List<String> values, Class clazz) {
        return values.stream().map(x -> toRealValue(x, clazz)).collect(Collectors.toList());
    }

    public static Object toRealValue(String value, Class clazz) {
        if (clazz.isAssignableFrom(String.class)) {
            return value;
        }
        if (clazz.isAssignableFrom(PolyString.class)) {
            return PolyString.fromOrig((String) value);
        }
        if (clazz.isAssignableFrom(Integer.class)) {
            return Integer.parseInt(value);
        }
        if (clazz.isAssignableFrom(Long.class)) {
            return Long.parseLong(value);
        }
        if (clazz.isAssignableFrom(ProtectedStringType.class)) {
            ProtectedStringType p = new ProtectedStringType();
            p.setClearValue(value);
            return p;
        }
        // TODO more type

        throw new UnsupportedOperationException(clazz + " is not supported");
    }

    public static PolicyError toPolicyError(PolicyViolationException e) {
        PolicyError.Builder builder = PolicyError.newBuilder();
        Message wrapper = null;

        LocalizableMessage msg = e.getUserFriendlyMessage();
        if (msg instanceof SingleLocalizableMessage) {
            wrapper = toMessage((SingleLocalizableMessage) msg);

        } else if (msg instanceof LocalizableMessageList) {
            wrapper = toMessage((LocalizableMessageList) msg);
        } else if (msg == null) {
            wrapper = toIterableMessages(new Object[]{e.getMessage()}).iterator().next();
        }

        if (wrapper == null) {
            throw new UnsupportedOperationException(msg.getClass() + " is not supported");
        }

        builder.setMessage(wrapper);

        return builder.build();
    }

    private static Message toMessage(LocalizableMessageList list) {
        MessageList messageList = toMessageList(list);
        return Message.newBuilder()
                .setList(messageList)
                .build();
    }

    private static MessageList toMessageList(LocalizableMessageList list) {
        MessageList.Builder builder = MessageList.newBuilder();

        for (LocalizableMessage msg : list.getMessages()) {
            Message wrapper = null;
            if (msg instanceof SingleLocalizableMessage) {
                wrapper = toMessage((SingleLocalizableMessage) msg);

            } else if (msg instanceof LocalizableMessageList) {
                wrapper = toMessage((LocalizableMessageList) msg);
            }

            if (wrapper == null) {
                throw new UnsupportedOperationException(msg.getClass() + " is not supported");
            }

            builder.addMessage(wrapper);
        }

        return builder.build();
    }

    private static Message toMessage(SingleLocalizableMessage msg) {
        return Message.newBuilder()
                .setSingle(toSingleMessage(msg))
                .build();
    }

    private static SingleMessage toSingleMessage(SingleLocalizableMessage msg) {
        return SingleMessage.newBuilder()
                .setKey(msg.getKey())
                .addAllArgs(toIterableMessages(msg.getArgs()))
                .build();
    }

    private static Iterable<? extends Message> toIterableMessages(Object[] args) {
        List<Message> list = new ArrayList<>();

        if (args == null) {
            return list;
        }

        for (Object arg : args) {
            Message wrapper;

            if (arg instanceof SingleLocalizableMessage) {
                wrapper = toMessage((SingleLocalizableMessage) arg);
            } else if (arg instanceof LocalizableMessageList) {
                wrapper = toMessage((LocalizableMessageList) arg);
            } else {
                wrapper = Message.newBuilder()
                        .setString(arg != null ? arg.toString() : "")
                        .build();
            }

            list.add(wrapper);
        }

        return list;
    }

    public static String toStringMessage(String string) {
        if (string == null) {
            return null;
        }
        return string;
    }

    public static BytesMessage toBytesMessage(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return BytesMessage.newBuilder()
                .setValue(ByteString.copyFrom(bytes))
                .build();
    }

    public static PolyStringMessage toPolyStringMessage(PolyString polyString) {
        if (polyString == null) {
            return null;
        }
        return PolyStringMessage.newBuilder()
                .setOrig(polyString.getOrig())
                .setNorm(polyString.getNorm())
                .build();
    }

    public static IntegerMessage toIntegerMessage(Integer value) {
        if (value == null) {
            return null;
        }
        return IntegerMessage.newBuilder()
                .setValue(value)
                .build();
    }

    public static LongMessage toLongMessage(Long value) {
        if (value == null) {
            return null;
        }
        return LongMessage.newBuilder()
                .setValue(value)
                .build();
    }

    public static PolyStringMessage toPolyStringMessage(PolyStringType polyStringType) {
        if (polyStringType == null) {
            return null;
        }
        return PolyStringMessage.newBuilder()
                .setOrig(polyStringType.getOrig())
                .setNorm(polyStringType.getNorm())
                .build();
    }

    public static List<PolyStringMessage> toPolyStringMessageList(List<PolyStringType> polyStringType) {
        return polyStringType.stream()
                .map(x -> toPolyStringMessage(x))
                .collect(Collectors.toList());
    }


    public static List<ReferenceMessage> toReferenceMessageList(List<ObjectReferenceType> ref, Map<String, AbstractRoleType> resolved) {
        return ref.stream()
                .map(x -> toReferenceMessage(x, resolved.get(x.getOid())))
                .collect(Collectors.toList());
    }

    private static ReferenceMessage toReferenceMessage(ItemDefinition definition, PrismReferenceValue value) {
        return toReferenceMessage((ObjectReferenceType) value.getRealValue(), null);
    }

    public static ReferenceMessage toReferenceMessage(ObjectReferenceType ref, AbstractRoleType resolved) {
        if (ref == null) {
            return null;
        }

        if (ref.getOid() == null) {
            LOGGER.warn("ref's oid is null, ref: {}", ref);
            return null;
        }

        QName type = ref.getType();
        QName relation = ref.getRelation();
        ReferenceMessage.Builder builder = BuilderWrapper.wrap(ReferenceMessage.newBuilder())
                .nullSafe(toPolyStringMessage(ref.getTargetName()), (b, v) -> b.setName(v))
                .unwrap()
                .setOid(ref.getOid())
                .setType(toQNameMessage(type))
                .setRelation(toQNameMessage(relation));
        if (resolved != null) {
            builder = BuilderWrapper.wrap(builder)
                    .nullSafe(toPolyStringMessage(resolved.getName()), (b, v) -> b.setName(v))
                    .nullSafe(toPolyStringMessage(resolved.getDisplayName()), (b, v) -> b.setDisplayName(v))
                    .nullSafe(toStringMessage(resolved.getDescription()), (b, v) -> b.setDescription(v))
                    .nullSafe(toStringMessage(resolved.getEmailAddress()), (b, v) -> b.setEmailAddress(v))
                    .unwrap();
        }

        return builder.build();
    }

    public static QNameMessage toQNameMessage(QName qname) {
        return QNameMessage.newBuilder()
                .setNamespaceURI(qname.getNamespaceURI())
                .setLocalPart(qname.getLocalPart())
                .setPrefix(qname.getPrefix())
                .build();
    }

    public static String toStringValue(String message) {
        if (message == null || message.isEmpty()) {
            return null;
        }
        return message;
    }

    public static byte[] toByteArrayValue(BytesMessage message) {
        if (message.getBytesOptionalCase() == BytesMessage.BytesOptionalCase.BYTESOPTIONAL_NOT_SET) {
            return null;
        }
        return message.getValue().toByteArray();
    }

    public static PolyStringType toPolyStringTypeValue(PolyStringMessage message) {
        if (isEmpty(message)) {
            return null;
        }
        return new PolyStringType(new PolyString(message.getOrig(), message.getNorm()));
    }

    public static Integer toIntegerValue(IntegerMessage message) {
        if (message.getIntOptionalCase() == IntegerMessage.IntOptionalCase.INTOPTIONAL_NOT_SET) {
            return null;
        }
        return message.getValue();
    }

    public static Long toLongValue(LongMessage message) {
        if (message.getLongOptionalCase() == LongMessage.LongOptionalCase.LONGOPTIONAL_NOT_SET) {
            return null;
        }
        return message.getValue();
    }

    public static List<? extends AssignmentType> toAssignmentTypeListValue(PrismContext prismContext, List<AssignmentMessage> assignmentList) {
        return assignmentList.stream()
                .map(x -> toAssignmentTypeValue(prismContext, x))
                .filter(x -> x != null)
                .collect(Collectors.toList());
    }

    public static AssignmentType toAssignmentTypeValue(PrismContext prismContext, AssignmentMessage message) {
        AssignmentType assignment = new AssignmentType();
        assignment.setTargetRef(toObjectReferenceTypeValue(prismContext, message.getTargetRef()));
        if (message.hasOrgRef()) {
            assignment.setOrgRef(toObjectReferenceTypeValue(prismContext, message.getOrgRef()));
        }
        if (!message.getExtensionMap().isEmpty()) {
            try {
                prismContext.adopt(assignment);
                addExtensionType(prismContext, assignment.asPrismContainerValue(), message.getExtensionMap());
            } catch (SchemaException e) {
                StatusRuntimeException exception = Status.INVALID_ARGUMENT
                        .withCause(e)
                        .withDescription("invalid_assignment_extension")
                        .asRuntimeException();
                throw exception;
            }
        }
        return assignment;
    }

    private static Class<? extends Containerable> toObjectClass(QName type) {
        // TODO midpoint has utility method for this converting?
        if (type == ObjectType.COMPLEX_TYPE) {
            return ObjectType.class;
        }
        if (type == FocusType.COMPLEX_TYPE) {
            return FocusType.class;
        }
        if (type == AssignmentHolderType.COMPLEX_TYPE) {
            return AssignmentHolderType.class;
        }
        if (type == UserType.COMPLEX_TYPE) {
            return UserType.class;
        }
        if (type == AbstractRoleType.COMPLEX_TYPE) {
            return AbstractRoleType.class;
        }
        if (type == RoleType.COMPLEX_TYPE) {
            return RoleType.class;
        }
        if (type == OrgType.COMPLEX_TYPE) {
            return OrgType.class;
        }
        if (type == ServiceType.COMPLEX_TYPE) {
            return ServiceType.class;
        }
        // TODO add more type converter

        return ObjectType.class;
    }

    public static ObjectQuery toObjectQuery(PrismContext prismContext, Class<? extends Containerable> queryClass,
                                            QueryMessage message) {
        S_FilterEntryOrEmpty builder = QueryBuilder.queryFor(queryClass, prismContext);
        S_FilterExit exitFilter = toObjectFilter(prismContext, builder, message.getFilter());

        S_QueryExit exitBuilder;
        if (exitFilter != null) {
            exitBuilder = exitFilter;
        } else {
            exitBuilder = builder;
        }

        ObjectQuery query = exitBuilder.build();
        query.setPaging(toObjectPaging(prismContext, message.getPaging()));
        return query;
    }

    public static RelationalValueSearchQuery toRelationalValueSearchQuery(PrismContext prismContext, RelationalValueSearchQueryMessage message) {
        RelationalValueSearchQuery query = new RelationalValueSearchQuery(toQNameValue(message.getColumn()), message.getSearchValue(),
                toSearchType(message.getSearchType()));
        if (message.hasPaging()) {
            query.setPaging(toObjectPaging(prismContext, message.getPaging()));
        }
        return query;
    }

    private static RelationalValueSearchType toSearchType(RelationalValueSearch message) {
        switch (message) {
            case EXACT:
                return RelationalValueSearchType.EXACT;
            case STARTS_WITH:
                return RelationalValueSearchType.STARTS_WITH;
            case SUBSTRING:
                return RelationalValueSearchType.SUBSTRING;
        }
        return RelationalValueSearchType.EXACT;
    }

    public static ObjectPaging toObjectPaging(PrismContext prismContext, PagingMessage message) {
        ObjectPaging paging = prismContext.queryFactory().createPaging();
        paging.setOffset(message.getOffset());
        if (message.getMaxSize() != 0) {
            paging.setMaxSize(message.getMaxSize());
        }
        paging.setOrdering(toObjectOrdering(prismContext, message.getOrderingList()));
        return paging;
    }

    public static Collection<? extends ObjectOrdering> toObjectOrdering(PrismContext prismContext, List<ObjectOrderingMessage> message) {
        return message.stream().map(x -> {
            OrderDirection direction = toOrderDirectionValue(x.getOrderDirection());
            String orderBy = x.getOrderBy();
            ItemPath itemPath = toItemPath(orderBy);

            return prismContext.queryFactory().createOrdering(itemPath, direction);
        }).collect(Collectors.toList());
    }

    private static OrderDirection toOrderDirectionValue(OrderDirectionType message) {
        switch (message) {
            case ASCENDING:
                return OrderDirection.ASCENDING;
            case DESCENDING:
                return OrderDirection.DESCENDING;
        }
        return OrderDirection.ASCENDING;
    }

    public static S_FilterExit toObjectFilter(PrismContext prismContext, S_FilterEntryOrEmpty builder, ObjectFilterMessage message) {
        if (message.hasAnd()) {
            return toAndQuery(prismContext, builder, message.getAnd());
        } else if (message.hasOr()) {
            return toOrQuery(prismContext, builder, message.getOr());
        } else if (message.hasNot()) {
            return toNotQuery(prismContext, builder, message.getNot());

        } else if (message.hasEq()) {
            return toEqFilter(builder, message.getEq());
        } else if (message.hasStartsWith()) {
            return toStartsWithFilter(builder, message.getStartsWith());
        } else if (message.hasContains()) {
            return toContainsFilter(builder, message.getContains());
        } else if (message.hasEndsWith()) {
            return toEndsWithFilter(builder, message.getEndsWith());

        } else if (message.hasEqPolyString()) {
            return toEqPolyFilter(builder, message.getEqPolyString());
        } else if (message.hasStartsWithPolyString()) {
            return toStartsWithPolyFilter(builder, message.getStartsWithPolyString());
        } else if (message.hasContainsPolyString()) {
            return toContainsPolyFilter(builder, message.getContainsPolyString());
        } else if (message.hasEndsWithPolyString()) {
            return toEndsWithPolyFilter(builder, message.getEndsWithPolyString());

        } else if (message.hasInOid()) {
            return toInOidFilter(prismContext, builder, message.getInOid());

        } else if (message.hasRef()) {
            return toRefFilter(prismContext, builder, message.getRef());

        } else if (message.hasOrgRoot()) {
            return toOrgRootFilter(prismContext, builder, message.getOrgRoot());
        } else if (message.hasOrgRef()) {
            return toOrgRefFilter(prismContext, builder, message.getOrgRef());
        }

        return null;
    }

    public static S_FilterExit toObjectFilter(PrismContext prismContext, S_FilterEntry builder, ObjectFilterMessage message) {
        if (message.hasAnd()) {
            return toAndQuery(prismContext, builder, message.getAnd());
        } else if (message.hasOr()) {
            return toOrQuery(prismContext, builder, message.getOr());
        }

        if (builder instanceof S_FilterEntryOrEmpty) {
            return toObjectFilter(prismContext, (S_FilterEntryOrEmpty) builder, message);
        }

        return null;
    }

    public static ItemPath toItemPath(FilterEntryMessage message) {
        return toItemPath(message.getFullPath());
    }

    public static ItemPath toItemPath(String message) {
        return ItemPath.create(Arrays.stream(message.split("/")).map(x -> {
            try {
                if (x.startsWith("[") && x.endsWith("]")) {
                    return Long.parseLong(x.substring(1, x.length() - 1));
                }
                return x;
            } catch (NumberFormatException e) {
                return x;
            }
        }).collect(Collectors.toList()));
    }

    public static S_FilterExit toEqFilter(S_FilterEntryOrEmpty builder, FilterEntryMessage message) {
        S_MatchingRuleEntry filter = builder.item(toItemPath(message))
                .eq(message.getValue());

        if (message.hasMatchingRule()) {
            return filter.matching(toQNameValue(message.getMatchingRule()));
        }
        return filter;
    }

    public static S_FilterExit toStartsWithFilter(S_FilterEntryOrEmpty builder, FilterEntryMessage message) {
        S_MatchingRuleEntry filter = builder.item(toItemPath(message))
                .startsWith(message.getValue());

        if (message.hasMatchingRule()) {
            return filter.matching(toQNameValue(message.getMatchingRule()));
        }
        return filter;
    }

    private static S_FilterExit toContainsFilter(S_FilterEntryOrEmpty builder, FilterEntryMessage message) {
        S_MatchingRuleEntry filter = builder.item(toItemPath(message))
                .contains(message.getValue());

        if (message.hasMatchingRule()) {
            return filter.matching(toQNameValue(message.getMatchingRule()));
        }
        return filter;
    }

    public static S_FilterExit toEndsWithFilter(S_FilterEntryOrEmpty builder, FilterEntryMessage message) {
        S_MatchingRuleEntry filter = builder.item(toItemPath(message))
                .endsWith(message.getValue());

        if (message.hasMatchingRule()) {
            return filter.matching(toQNameValue(message.getMatchingRule()));
        }
        return filter;
    }

    public static S_FilterExit toEqPolyFilter(S_FilterEntryOrEmpty builder, FilterEntryMessage message) {
        S_MatchingRuleEntry filter = builder.item(toItemPath(message))
                .eqPoly(message.getValue());

        if (message.hasMatchingRule()) {
            return filter.matching(toQNameValue(message.getMatchingRule()));
        }
        return filter;
    }

    public static S_FilterExit toStartsWithPolyFilter(S_FilterEntryOrEmpty builder, FilterEntryMessage message) {
        S_MatchingRuleEntry filter = builder.item(toItemPath(message))
                .startsWithPoly(message.getValue());

        if (message.hasMatchingRule()) {
            return filter.matching(toQNameValue(message.getMatchingRule()));
        }
        return filter;
    }

    private static S_FilterExit toContainsPolyFilter(S_FilterEntryOrEmpty builder, FilterEntryMessage message) {
        S_MatchingRuleEntry filter = builder.item(toItemPath(message))
                .containsPoly(message.getValue());

        if (message.hasMatchingRule()) {
            return filter.matching(toQNameValue(message.getMatchingRule()));
        }
        return filter;
    }

    public static S_FilterExit toEndsWithPolyFilter(S_FilterEntryOrEmpty builder, FilterEntryMessage message) {
        S_MatchingRuleEntry filter = builder.item(toItemPath(message))
                .endsWithPoly(message.getValue());

        if (message.hasMatchingRule()) {
            return filter.matching(toQNameValue(message.getMatchingRule()));
        }
        return filter;
    }

    public static S_FilterExit toAndQuery(PrismContext prismContext, S_FilterEntry builder, AndFilterMessage message) {
        List<ObjectFilterMessage> list = message.getConditionsList();

        for (int i = 0; i < list.size(); i++) {
            ObjectFilterMessage filter = list.get(i);
            S_FilterExit f = toObjectFilter(prismContext, builder, filter);

            if (i < list.size() - 1) {
                builder = f.and();
            } else {
                return f;
            }
        }
        return null;
    }

    public static S_FilterExit toOrQuery(PrismContext prismContext, S_FilterEntry builder, OrFilterMessage message) {
        List<ObjectFilterMessage> list = message.getConditionsList();

        for (int i = 0; i < list.size(); i++) {
            ObjectFilterMessage filter = list.get(i);
            S_FilterExit f = toObjectFilter(prismContext, builder, filter);

            if (i < list.size() - 1) {
                builder = f.or();
            } else {
                return f;
            }
        }
        return null;
    }

    public static S_FilterExit toNotQuery(PrismContext prismContext, S_FilterEntryOrEmpty builder, NotFilterMessage message) {
        S_FilterEntry not = builder.not();
        S_FilterEntryOrEmpty block = not.block();
        S_FilterExit q = toObjectFilter(prismContext, block, message.getFilter());
        return q.endBlock();
    }

    public static SearchFilterType toEqFilter(PrismContext prismContext, QName type, QName findKey, PolyStringMessage message) {
        Class<? extends Containerable> queryClass = toObjectClass(type);

        ObjectFilter filter = QueryBuilder.queryFor(queryClass, prismContext)
                .item(findKey)
                .eqPoly(message.getOrig())
                .matchingOrig()
                .build()
                .getFilter();

        try {
            return prismContext.getQueryConverter().createSearchFilterType(filter);
        } catch (SchemaException e) {
            StatusRuntimeException exception = Status.INVALID_ARGUMENT
                    .withCause(e)
                    .withDescription("invalid_filter")
                    .asRuntimeException();
            throw exception;
        }
    }

    public static SearchFilterType toEqFilter(PrismContext prismContext, QName type, QName findKey, String message, QName matchingRule) {
        Class<? extends Containerable> queryClass = toObjectClass(type);

        ObjectFilter filter = QueryBuilder.queryFor(queryClass, prismContext)
                .item(findKey)
                .eq(message)
                .matching(matchingRule)
                .build()
                .getFilter();

        try {
            return prismContext.getQueryConverter().createSearchFilterType(filter);
        } catch (SchemaException e) {
            StatusRuntimeException exception = Status.INVALID_ARGUMENT
                    .withCause(e)
                    .withDescription("invalid_filter")
                    .asRuntimeException();
            throw exception;
        }
    }

    public static S_FilterExit toInOidFilter(PrismContext prismContext, S_FilterEntryOrEmpty builder, FilterInOidMessage message) {
        return builder.id(message.getValueList().toArray(new String[0]));
    }

    public static S_FilterExit toRefFilter(PrismContext prismContext, S_FilterEntryOrEmpty builder, FilterReferenceMessage message) {
        ObjectReferenceType ref = toObjectReferenceTypeValue(prismContext, message.getValue());
        return builder.item(toItemPath(message))
                .ref(ref.asReferenceValue());
    }

    public static S_FilterExit toOrgRootFilter(PrismContext prismContext, S_FilterEntryOrEmpty builder, FilterOrgRootMessage message) {
        if (message.getIsRoot()) {
            return builder.isRoot();
        }
        // TODO Need fix? This is not same behaviour with midPoint isRoot filter which returns
        // "No organization reference defined in the search query." error.
        return builder.undefined();
    }

    public static S_FilterExit toOrgRefFilter(PrismContext prismContext, S_FilterEntryOrEmpty builder, FilterOrgReferenceMessage message) {
        ObjectReferenceType ref = toObjectReferenceTypeValue(prismContext, message.getValue());
        return builder.isInScopeOf(ref.asReferenceValue(), toOrgFilterScope(message.getScope()));
    }

    public static OrgFilter.Scope toOrgFilterScope(OrgFilterScope scope) {
        switch (scope) {
            case ONE_LEVEL:
                return OrgFilter.Scope.ONE_LEVEL;
            case SUBTREE:
                return OrgFilter.Scope.SUBTREE;
            case ANCESTORS:
                return OrgFilter.Scope.ANCESTORS;
        }
        return null;
    }

    public static ItemPath toItemPath(FilterReferenceMessage message) {
        return toItemPath(message.getFullPath());
    }

    public static ObjectReferenceType toObjectReferenceTypeValue(PrismContext prismContext, ReferenceMessage message) {
        ObjectReferenceType ref = new ObjectReferenceType();

        if (message.hasType()) {
            ref.setType(toQNameValue(message.getType()));
        } else if (message.getTypeWrapperCase() == ReferenceMessage.TypeWrapperCase.OBJECT_TYPE) {
            ref.setType(toQNameValue(message.getObjectType()));
        } else {
            // Don't set reference type
        }

        QName type = ref.getType();

        // Find the target by name, emailAddress or oid
        if (message.hasName()) {
            ref.setFilter(toEqFilter(prismContext, type, ObjectType.F_NAME, message.getName()));
        } else if (!message.getEmailAddress().isEmpty()) {
            ref.setFilter(toEqFilter(prismContext, type, FocusType.F_EMAIL_ADDRESS, message.getEmailAddress(), PrismConstants.STRING_IGNORE_CASE_MATCHING_RULE_NAME));
        } else if (!message.getOid().isEmpty()) {
            ref.setOid(message.getOid());
        } else {
            return null;
        }

        if (message.hasRelation()) {
            ref.setRelation(toQNameValue(message.getRelation()));
        } else if (message.getRelationWrapperCase() == ReferenceMessage.RelationWrapperCase.RELATION_TYPE) {
            ref.setRelation(toQNameValue(message.getRelationType()));
        } else {
            // Use default relation if missing relation
            ref.setRelation(prismContext.getDefaultRelation());
        }

        return ref;
    }


//    private static ObjectQuery createSearchByName(PrismContext prismContext, Class<? extends Containerable> queryClass, PolyStringMessage message)
//            throws SchemaException {
//        if (!message.getOrig().isEmpty()) {
//            return prismContext.queryFor(queryClass)
//                    .item(ObjectType.F_NAME)
//                    .eq(message.getOrig())
//                    .matching(PrismConstants.POLY_STRING_ORIG_MATCHING_RULE_NAME)
//                    .build();
//        } else if (!message.getNorm().isEmpty()) {
//            return prismContext.queryFor(queryClass)
//                    .item(ObjectType.F_NAME)
//                    .eq(message.getNorm())
//                    .matching(PrismConstants.POLY_STRING_NORM_MATCHING_RULE_NAME)
//                    .build();
//        }
//        return null;
//    }

    public static QName toQNameValue(QNameMessage message) {
        if (message.getNamespaceURI().isEmpty() && message.getLocalPart().isEmpty()) {
            return SchemaConstants.ORG_DEFAULT;
        }

        if (message.getNamespaceURI().isEmpty()) {
            return new QName(message.getLocalPart());
        }
        if (message.getPrefix().isEmpty()) {
            return new QName(message.getNamespaceURI(), message.getLocalPart());
        }
        return new QName(message.getNamespaceURI(), message.getLocalPart(), message.getPrefix());
    }

    public static QName toQNameValue(DefaultRelationType type) {
        switch (type) {
            case ORG_DEFAULT:
                return SchemaConstants.ORG_DEFAULT;
            case ORG_MANAGER:
                return SchemaConstants.ORG_MANAGER;
            case ORG_META:
                return SchemaConstants.ORG_META;
            case ORG_DEPUTY:
                return SchemaConstants.ORG_DEPUTY;
            case ORG_APPROVER:
                return SchemaConstants.ORG_APPROVER;
            case ORG_OWNER:
                return SchemaConstants.ORG_OWNER;
            case ORG_CONSENT:
                return SchemaConstants.ORG_CONSENT;
        }
        return null;
    }

    public static QName toQNameValue(DefaultObjectType type) {
        switch (type) {
            case OBJECT_TYPE:
                return ObjectType.COMPLEX_TYPE;
            case FOCUS_TYPE:
                return FocusType.COMPLEX_TYPE;
            case USER_TYPE:
                return UserType.COMPLEX_TYPE;
            case ABSTRACT_ROLE_TYPE:
                return AbstractRoleType.COMPLEX_TYPE;
            case ROLE_TYPE:
                return RoleType.COMPLEX_TYPE;
            case ORG_TYPE:
                return OrgType.COMPLEX_TYPE;
            case SERVICE_TYPE:
                return ServiceType.COMPLEX_TYPE;
            case ASSIGNMENT_HOLDER_TYPE:
                return AssignmentHolderType.COMPLEX_TYPE;
            case ARCHETYPE_TYPE:
                return ArchetypeType.COMPLEX_TYPE;
            case LOOKUP_TABLE_TYPE:
                return LookupTableType.COMPLEX_TYPE;
        }
        return null;
    }

    public static boolean isEmpty(PolyStringMessage message) {
        if (message.getNorm().isEmpty() && message.getOrig().isEmpty()) {
            return true;
        }
        return false;
    }

    public static void addExtensionType(PrismContext prismContext, ObjectType object, Map<String, ItemMessage> map) throws SchemaException {
        if (map.isEmpty()) {
            return;
        }

        addExtensionType(prismContext, object.asPrismObject(), map);
    }

    public static void addExtensionType(PrismContext prismContext, PrismObject object, Map<String, ItemMessage> map) throws SchemaException {
        if (map.isEmpty()) {
            return;
        }

        PrismContainer extension = object.findOrCreateContainer(ObjectType.F_EXTENSION);
        addExtensionType(prismContext, extension, map);
    }

    public static void addExtensionType(PrismContext prismContext, PrismContainerValue object, Map<String, ItemMessage> map) throws SchemaException {
        if (map.isEmpty()) {
            return;
        }

        PrismContainer extension = object.findOrCreateContainer(ObjectType.F_EXTENSION);
        addExtensionType(prismContext, extension, map);
    }

    public static void addExtensionType(PrismContext prismContext, PrismContainer extension, Map<String, ItemMessage> map) throws SchemaException {
        if (map.isEmpty()) {
            return;
        }

        for (Map.Entry<String, ItemMessage> entry : map.entrySet()) {
            String k = entry.getKey();
            ItemMessage v = entry.getValue();

            QName qname = toQName(v, k);
            ItemPath path = ItemPath.create(qname);

            ItemDefinition definition = extension.getDefinition().findItemDefinition(path);
            if (definition == null) {
                throw new SchemaException("No schema: " + qname);
            }

            if (definition instanceof PrismPropertyDefinition) {
                addExtensionValue(prismContext, extension, (PrismPropertyDefinition) definition, path, v.getProperty());

            } else if (definition instanceof PrismContainerDefinition) {
                addExtensionValue(prismContext, extension, (PrismContainerDefinition) definition, path, v.getContainer());

            } else if (definition instanceof PrismReferenceDefinition) {
                addExtensionValue(prismContext, extension, (PrismReferenceDefinition) definition, path, v.getRef());

            } else {
                throw new UnsupportedOperationException(definition.getClass() + " is not supported");
            }
        }
    }

    private static void addExtensionValue(PrismContext prismContext, PrismContainer extension, PrismReferenceDefinition definition,
                                          ItemPath path, PrismReferenceMessage ref) throws SchemaException {
        // Empty, do nothing
        if (ref == null) {
            return;
        }

        PrismReference holder = extension.findOrCreateReference(path);

        if (definition.isSingleValue() && ref.getValuesCount() > 1) {
            throw new SchemaException("Multiple value is used for single value: " + path);
        }

        for (ReferenceMessage value : ref.getValuesList()) {
            holder.add(toPrismReferenceValue(prismContext, definition, value));
        }
    }

    private static void addExtensionValue(PrismContext prismContext, PrismContainer extension, PrismContainerDefinition definition,
                                          ItemPath path, PrismContainerMessage container) throws SchemaException {
        // Empty, do nothing
        if (container.getValuesCount() == 0) {
            return;
        }

        PrismContainer holder = extension.findOrCreateContainer(path);

        if (definition.isSingleValue() && container.getValuesCount() > 1) {
            throw new SchemaException("Multiple value is used for single value: " + path);
        }

        for (PrismContainerValueMessage value : container.getValuesList()) {
            holder.add(toPrismContainerValue(prismContext, definition, value));
        }
    }

    private static void addExtensionValue(PrismContext prismContext, PrismContainer extension, PrismPropertyDefinition definition,
                                          ItemPath path, PrismPropertyMessage property) throws SchemaException {
        // Empty, do nothing
        if (property.getValuesCount() == 0) {
            return;
        }

        PrismProperty holder = extension.findOrCreateProperty(path);

        if (definition.isSingleValue() && property.getValuesCount() > 1) {
            throw new SchemaException("Multiple value is used for single value: " + path);
        }

        for (PrismPropertyValueMessage value : property.getValuesList()) {
            holder.add(toPrismPropertyValue(prismContext, definition, value));
        }
    }

    public static QName toQName(ItemMessage message, String localPart) {
        if (message.getItemName().getNamespaceURI().isEmpty()) {
            return new QName(localPart);
        }
        return new QName(message.getItemName().getNamespaceURI(), localPart, message.getItemName().getPrefix());
    }

    public static QName toQName(PrismValueMessage message, String localPart) {
        if (message.getNamespaceURI().isEmpty()) {
            return new QName(localPart);
        }
        return new QName(message.getNamespaceURI(), localPart);
    }

    public static PrismObject<UserType> toPrismObject(PrismContext prismContext, RepositoryService repo, UserTypeMessage message) throws SchemaException {
        UserType user = new UserType(prismContext);
        PrismContainerDefinition definition = user.asPrismContainerValue().getDefinition();

        // ObjectType
        user.setName(toPolyStringTypeValue(message.getName()));
        user.setDescription(toStringValue(message.getDescription()));
        user.getSubtype().addAll(message.getSubtypeList());
        user.setLifecycleState(toStringValue(message.getLifecycleState()));

        // AssignmentHolderType
        user.createAssignmentList().addAll(toAssignmentTypeListValue(prismContext, message.getAssignmentList()));
        // can't set Archetype here directly because it throws policy error always.

        // FocusType
        user.setJpegPhoto(toByteArrayValue(message.getJpegPhoto()));
        user.setCostCenter(toStringValue(message.getCostCenter()));
        user.setLocality((toPolyStringTypeValue(message.getLocality())));
        user.setPreferredLanguage(toStringValue(message.getPreferredLanguage()));
        user.setLocale(toStringValue(message.getLocale()));
        user.setTimezone(toStringValue(message.getTimezone()));
        user.setEmailAddress(toStringValue(message.getEmailAddress()));
        user.setTelephoneNumber(toStringValue(message.getTelephoneNumber()));

        // UserType
        user.setFullName(toPolyStringTypeValue(message.getFullName()));
        user.setGivenName(toPolyStringTypeValue(message.getGivenName()));
        user.setFamilyName(toPolyStringTypeValue(message.getFamilyName()));
        user.setAdditionalName(toPolyStringTypeValue(message.getAdditionalName()));
        user.setNickName(toPolyStringTypeValue(message.getNickName()));
        user.setHonorificPrefix(toPolyStringTypeValue(message.getHonorificPrefix()));
        user.setHonorificSuffix(toPolyStringTypeValue(message.getHonorificSuffix()));
        user.setTitle(toPolyStringTypeValue(message.getTitle()));
        user.setEmployeeNumber(toStringValue(message.getEmployeeNumber()));
        user.getOrganization().addAll(toPolyStringTypeValueList(message.getOrganizationList()));
        user.getOrganizationalUnit().addAll(toPolyStringTypeValueList(message.getOrganizationalUnitList()));

        // Extension
        addExtensionType(prismContext, user, message.getExtensionMap());

        return user.asPrismObject();
    }

    public static PrismObject<RoleType> toPrismObject(PrismContext prismContext, RepositoryService repo, RoleTypeMessage message) throws SchemaException {
        RoleType object = new RoleType(prismContext);

        // ObjectType
        object.setName(toPolyStringTypeValue(message.getName()));
        object.setDescription(toStringValue(message.getDescription()));
        object.getSubtype().addAll(message.getSubtypeList());
        object.setLifecycleState(toStringValue(message.getLifecycleState()));

        // AssignmentHolderType
        object.createAssignmentList().addAll(toAssignmentTypeListValue(prismContext, message.getAssignmentList()));
        // can't set Archetype here directly because it throws policy error always.

        // FocusType
        object.setJpegPhoto(toByteArrayValue(message.getJpegPhoto()));
        object.setCostCenter(toStringValue(message.getCostCenter()));
        object.setLocality((toPolyStringTypeValue(message.getLocality())));
        object.setPreferredLanguage(toStringValue(message.getPreferredLanguage()));
        object.setLocale(toStringValue(message.getLocale()));
        object.setTimezone(toStringValue(message.getTimezone()));
        object.setEmailAddress(toStringValue(message.getEmailAddress()));
        object.setTelephoneNumber(toStringValue(message.getTelephoneNumber()));

        // AbstractRoleType
        object.setDisplayName(toPolyStringTypeValue(message.getDisplayName()));
        object.setIdentifier(toStringValue(message.getIdentifier()));
        object.setRequestable(toBooleanValue(message.getRequestable()));
        object.setDelegable(toBooleanValue(message.getDelegable()));
        object.setRiskLevel(toStringValue(message.getRiskLevel()));

        // RoleType
//        object.setRoleType(toStringValue(message.getRoleType()));

        // Extension
        addExtensionType(prismContext, object, message.getExtensionMap());

        return object.asPrismObject();
    }

    public static PrismObject<OrgType> toPrismObject(PrismContext prismContext, RepositoryService repo, OrgTypeMessage message) throws SchemaException {
        OrgType object = new OrgType(prismContext);

        // ObjectType
        object.setName(toPolyStringTypeValue(message.getName()));
        object.setDescription(toStringValue(message.getDescription()));
        object.getSubtype().addAll(message.getSubtypeList());
        object.setLifecycleState(toStringValue(message.getLifecycleState()));

        // AssignmentHolderType
        object.createAssignmentList().addAll(toAssignmentTypeListValue(prismContext, message.getAssignmentList()));
        // can't set Archetype here directly because it throws policy error always.

        // FocusType
        object.setJpegPhoto(toByteArrayValue(message.getJpegPhoto()));
        object.setCostCenter(toStringValue(message.getCostCenter()));
        object.setLocality((toPolyStringTypeValue(message.getLocality())));
        object.setPreferredLanguage(toStringValue(message.getPreferredLanguage()));
        object.setLocale(toStringValue(message.getLocale()));
        object.setTimezone(toStringValue(message.getTimezone()));
        object.setEmailAddress(toStringValue(message.getEmailAddress()));
        object.setTelephoneNumber(toStringValue(message.getTelephoneNumber()));

        // AbstractRoleType
        object.setDisplayName(toPolyStringTypeValue(message.getDisplayName()));
        object.setIdentifier(toStringValue(message.getIdentifier()));
        object.setRequestable(toBooleanValue(message.getRequestable()));
        object.setDelegable(toBooleanValue(message.getDelegable()));
        object.setRiskLevel(toStringValue(message.getRiskLevel()));

        // OrgType
//        object.createOrgTypeList().addAll(message.getOrgTypeList());
        object.setTenant(toBooleanValue(message.getTenant()));
        object.getSubtype().addAll(message.getMailDomainList());
        object.setDisplayOrder(toIntValue(message.getDisplayOrder()));

        // Extension
        addExtensionType(prismContext, object, message.getExtensionMap());

        return object.asPrismObject();
    }

    public static PrismObject<ServiceType> toPrismObject(PrismContext prismContext, RepositoryService repo, ServiceTypeMessage message) throws SchemaException {
        ServiceType object = new ServiceType(prismContext);

        // ObjectType
        object.setName(toPolyStringTypeValue(message.getName()));
        object.setDescription(toStringValue(message.getDescription()));
        object.getSubtype().addAll(message.getSubtypeList());
        object.setLifecycleState(toStringValue(message.getLifecycleState()));

        // AssignmentHolderType
        object.createAssignmentList().addAll(toAssignmentTypeListValue(prismContext, message.getAssignmentList()));
        // can't set Archetype here directly because it throws policy error always.

        // FocusType
        object.setJpegPhoto(toByteArrayValue(message.getJpegPhoto()));
        object.setCostCenter(toStringValue(message.getCostCenter()));
        object.setLocality((toPolyStringTypeValue(message.getLocality())));
        object.setPreferredLanguage(toStringValue(message.getPreferredLanguage()));
        object.setLocale(toStringValue(message.getLocale()));
        object.setTimezone(toStringValue(message.getTimezone()));
        object.setEmailAddress(toStringValue(message.getEmailAddress()));
        object.setTelephoneNumber(toStringValue(message.getTelephoneNumber()));

        // AbstractRoleType
        object.setDisplayName(toPolyStringTypeValue(message.getDisplayName()));
        object.setIdentifier(toStringValue(message.getIdentifier()));
        object.setRequestable(toBooleanValue(message.getRequestable()));
        object.setDelegable(toBooleanValue(message.getDelegable()));
        object.setRiskLevel(toStringValue(message.getRiskLevel()));

        // ServiceType
//        object.createServiceTypeList().addAll(message.getServiceTypeList());
        object.setDisplayOrder(toIntValue(message.getDisplayOrder()));
        object.setUrl(toStringValue(message.getUrl()));

        // Extension
        addExtensionType(prismContext, object, message.getExtensionMap());

        return object.asPrismObject();
    }

    private static Integer toIntValue(int displayOrder) {
        if (displayOrder == 0) {
            return null;
        }
        return displayOrder;
    }

    private static Boolean toBooleanValue(boolean value) {
        if (!value) {
            return null;
        }
        return true;
    }

    public static PrismObject toPrismObject(PrismContext prismContext, Class clazz,
                                            PrismContainerMessage message) throws SchemaException {

        PrismContainerDefinition<ObjectType> definition = prismContext.getSchemaRegistry()
                .findContainerDefinitionByCompileTimeClass(clazz);

        // Create objectType instance with requested objectType class
        PrismContainer<ObjectType> instantiate = definition.instantiate();
        ObjectType objectType = instantiate.getRealValue();

        // PrismObject must one value value
        List<PrismContainerValueMessage> values = message.getValuesList();
        if (values.size() > 1) {
            StatusRuntimeException exception = Status.INVALID_ARGUMENT
                    .withDescription("invalid_schema: PrismObject with more than one value")
                    .asRuntimeException();
            throw exception;
        }

        PrismObject<? extends ObjectType> prismObject = objectType.asPrismObject();

        if (values.size() > 0) {
            // Convert PrismContainerValueMessage to PrismObjectValue
            PrismObjectValue prismObjectValue = toPrismObjectValue(prismContext, definition, values.get(0), objectType);
            prismObject.setValue(prismObjectValue);
        }

        return prismObject;
    }

    private static List<PolyStringType> toPolyStringTypeValueList(List<PolyStringMessage> organizationList) {
        return organizationList.stream().map(x -> toPolyStringTypeValue(x)).collect(Collectors.toList());
    }

    public static UserTypeMessage toUserTypeMessage(UserType u, Collection<SelectorOptions<GetOperationOptions>> options) {
        return BuilderWrapper.wrap(UserTypeMessage.newBuilder())
                // ObjectType
                .nullSafe(u.getOid(), (b, v) -> b.setOid(v))
                .nullSafe(u.getVersion(), (b, v) -> b.setVersion(v))
                .nullSafe(toPolyStringMessage(u.getName()), (b, v) -> b.setName(v))
                .selectOptions(options)
                .nullSafeWithRetrieve(UserType.F_DESCRIPTION, u.getDescription(), (b, v) -> b.setDescription(v))
                .nullSafeWithRetrieve(UserType.F_SUBTYPE, u.getSubtype(), (b, v) -> b.addAllSubtype(v))
                .nullSafeWithRetrieve(UserType.F_LIFECYCLE_STATE, u.getLifecycleState(), (b, v) -> b.setLifecycleState(v))
                .nullSafeWithRetrieve(UserType.F_PARENT_ORG_REF, u.getParentOrgRef(), (b, v) -> {
                    return b.addAllParentOrgRef(v.stream().map(a -> {
                        return toReferenceMessage(a, null);
                    }).collect(Collectors.toList()));
                })
                // AssignmentHolderType
                .nullSafeWithRetrieve(UserType.F_ASSIGNMENT, u.getAssignment(), (b, v) -> {
                    return b.addAllAssignment(v.stream().map(a -> {
                        return AssignmentMessage.newBuilder()
                                .setTargetRef(
                                        toReferenceMessage(a.getTargetRef(), null)
                                ).build();
                    }).collect(Collectors.toList()));
                })
                .nullSafeWithRetrieve(UserType.F_ARCHETYPE_REF, u.getArchetypeRef(), (b, v) -> {
                    return b.addAllArchetypeRef(v.stream().map(a -> {
                        return toReferenceMessage(a, null);
                    }).collect(Collectors.toList()));
                })
                .nullSafeWithRetrieve(UserType.F_ROLE_MEMBERSHIP_REF, u.getRoleMembershipRef(), (b, v) -> {
                    return b.addAllRoleMembershipRef(v.stream().map(a -> {
                        return toReferenceMessage(a, null);
                    }).collect(Collectors.toList()));
                })
                // FocusType
                .nullSafeWithRetrieve(UserType.F_JPEG_PHOTO, toBytesMessage(u.getJpegPhoto()), (b, v) -> b.setJpegPhoto(v))
                .nullSafeWithRetrieve(UserType.F_COST_CENTER, u.getCostCenter(), (b, v) -> b.setCostCenter(v))
                .nullSafeWithRetrieve(UserType.F_LOCALITY, toPolyStringMessage(u.getLocality()), (b, v) -> b.setLocality(v))
                .nullSafeWithRetrieve(UserType.F_PREFERRED_LANGUAGE, u.getPreferredLanguage(), (b, v) -> b.setPreferredLanguage(v))
                .nullSafeWithRetrieve(UserType.F_LOCALE, u.getLocale(), (b, v) -> b.setLocale(v))
                .nullSafeWithRetrieve(UserType.F_TIMEZONE, u.getTimezone(), (b, v) -> b.setTimezone(v))
                .nullSafeWithRetrieve(UserType.F_EMAIL_ADDRESS, u.getEmailAddress(), (b, v) -> b.setEmailAddress(v))
                .nullSafeWithRetrieve(UserType.F_TELEPHONE_NUMBER, u.getTelephoneNumber(), (b, v) -> b.setTelephoneNumber(v))
                // UserType
                .nullSafeWithRetrieve(UserType.F_FULL_NAME, toPolyStringMessage(u.getFullName()), (b, v) -> b.setFullName(v))
                .nullSafeWithRetrieve(UserType.F_GIVEN_NAME, toPolyStringMessage(u.getGivenName()), (b, v) -> b.setGivenName(v))
                .nullSafeWithRetrieve(UserType.F_FAMILY_NAME, toPolyStringMessage(u.getFamilyName()), (b, v) -> b.setFamilyName(v))
                .nullSafeWithRetrieve(UserType.F_ADDITIONAL_NAME, toPolyStringMessage(u.getAdditionalName()), (b, v) -> b.setAdditionalName(v))
                .nullSafeWithRetrieve(UserType.F_NICK_NAME, toPolyStringMessage(u.getNickName()), (b, v) -> b.setNickName(v))
                .nullSafeWithRetrieve(UserType.F_HONORIFIC_PREFIX, toPolyStringMessage(u.getHonorificPrefix()), (b, v) -> b.setHonorificPrefix(v))
                .nullSafeWithRetrieve(UserType.F_HONORIFIC_SUFFIX, toPolyStringMessage(u.getHonorificSuffix()), (b, v) -> b.setHonorificSuffix(v))
                .nullSafeWithRetrieve(UserType.F_TITLE, toPolyStringMessage(u.getTitle()), (b, v) -> b.setTitle(v))
                .nullSafeWithRetrieve(UserType.F_EMPLOYEE_NUMBER, u.getEmployeeNumber(), (b, v) -> b.setEmployeeNumber(v))
                .nullSafeWithRetrieve(UserType.F_ORGANIZATION, toPolyStringMessageList(u.getOrganization()), (b, v) -> b.addAllOrganization(v))
                .nullSafeWithRetrieve(UserType.F_ORGANIZATIONAL_UNIT, toPolyStringMessageList(u.getOrganizationalUnit()), (b, v) -> b.addAllOrganizationalUnit(v))
                // Extension
                .nullSafeWithRetrieve(UserType.F_EXTENSION, u.getExtension(),
                        (v, ops, hasInclude) -> toItemMessageMap(v, ops, hasInclude),
                        (b, v) -> b.putAllExtension(v))
                .unwrap()
                .build();
    }

    public static RoleTypeMessage toRoleTypeMessage(RoleType u, Collection<SelectorOptions<GetOperationOptions>> options) {
        return BuilderWrapper.wrap(RoleTypeMessage.newBuilder())
                // ObjectType
                .nullSafe(u.getOid(), (b, v) -> b.setOid(v))
                .nullSafe(u.getVersion(), (b, v) -> b.setVersion(v))
                .nullSafe(toPolyStringMessage(u.getName()), (b, v) -> b.setName(v))
                .selectOptions(options)
                .nullSafeWithRetrieve(RoleType.F_DESCRIPTION, u.getDescription(), (b, v) -> b.setDescription(v))
                .nullSafeWithRetrieve(RoleType.F_SUBTYPE, u.getSubtype(), (b, v) -> b.addAllSubtype(v))
                .nullSafeWithRetrieve(RoleType.F_LIFECYCLE_STATE, u.getLifecycleState(), (b, v) -> b.setLifecycleState(v))
                .nullSafeWithRetrieve(RoleType.F_PARENT_ORG_REF, u.getParentOrgRef(), (b, v) -> {
                    return b.addAllParentOrgRef(v.stream().map(a -> {
                        return toReferenceMessage(a, null);
                    }).collect(Collectors.toList()));
                })
                // AssignmentHolderType
                .nullSafeWithRetrieve(RoleType.F_ASSIGNMENT, u.getAssignment(), (b, v) -> {
                    return b.addAllAssignment(v.stream().map(a -> {
                        return AssignmentMessage.newBuilder()
                                .setTargetRef(
                                        toReferenceMessage(a.getTargetRef(), null)
                                ).build();
                    }).collect(Collectors.toList()));
                })
                .nullSafeWithRetrieve(RoleType.F_ARCHETYPE_REF, u.getArchetypeRef(), (b, v) -> {
                    return b.addAllArchetypeRef(v.stream().map(a -> {
                        return toReferenceMessage(a, null);
                    }).collect(Collectors.toList()));
                })
                .nullSafeWithRetrieve(RoleType.F_ROLE_MEMBERSHIP_REF, u.getRoleMembershipRef(), (b, v) -> {
                    return b.addAllRoleMembershipRef(v.stream().map(a -> {
                        return toReferenceMessage(a, null);
                    }).collect(Collectors.toList()));
                })
                // FocusType
                .nullSafeWithRetrieve(RoleType.F_JPEG_PHOTO, toBytesMessage(u.getJpegPhoto()), (b, v) -> b.setJpegPhoto(v))
                .nullSafeWithRetrieve(RoleType.F_COST_CENTER, u.getCostCenter(), (b, v) -> b.setCostCenter(v))
                .nullSafeWithRetrieve(RoleType.F_LOCALITY, toPolyStringMessage(u.getLocality()), (b, v) -> b.setLocality(v))
                .nullSafeWithRetrieve(RoleType.F_PREFERRED_LANGUAGE, u.getPreferredLanguage(), (b, v) -> b.setPreferredLanguage(v))
                .nullSafeWithRetrieve(RoleType.F_LOCALE, u.getLocale(), (b, v) -> b.setLocale(v))
                .nullSafeWithRetrieve(RoleType.F_TIMEZONE, u.getTimezone(), (b, v) -> b.setTimezone(v))
                .nullSafeWithRetrieve(RoleType.F_EMAIL_ADDRESS, u.getEmailAddress(), (b, v) -> b.setEmailAddress(v))
                .nullSafeWithRetrieve(RoleType.F_TELEPHONE_NUMBER, u.getTelephoneNumber(), (b, v) -> b.setTelephoneNumber(v))
                // AbstractRoleType
                .nullSafeWithRetrieve(RoleType.F_DISPLAY_NAME, toPolyStringMessage(u.getDisplayName()), (b, v) -> b.setDisplayName(v))
                .nullSafeWithRetrieve(RoleType.F_IDENTIFIER, u.getIdentifier(), (b, v) -> b.setIdentifier(v))
                .nullSafeWithRetrieve(RoleType.F_REQUESTABLE, u.isRequestable(), (b, v) -> b.setRequestable(v))
                .nullSafeWithRetrieve(RoleType.F_DELEGABLE, u.isDelegable(), (b, v) -> b.setDelegable(v))
                .nullSafeWithRetrieve(RoleType.F_RISK_LEVEL, u.getRiskLevel(), (b, v) -> b.setRiskLevel(v))
                // RoleType
                // Extension
                .nullSafeWithRetrieve(RoleType.F_EXTENSION, u.getExtension(),
                        (v, ops, hasInclude) -> toItemMessageMap(v, ops, hasInclude),
                        (b, v) -> b.putAllExtension(v))
                .unwrap()
                .build();
    }

    public static OrgTypeMessage toOrgTypeMessage(OrgType u, Collection<SelectorOptions<GetOperationOptions>> options) {
        return BuilderWrapper.wrap(OrgTypeMessage.newBuilder())
                // ObjectType
                .nullSafe(u.getOid(), (b, v) -> b.setOid(v))
                .nullSafe(u.getVersion(), (b, v) -> b.setVersion(v))
                .nullSafe(toPolyStringMessage(u.getName()), (b, v) -> b.setName(v))
                .selectOptions(options)
                .nullSafeWithRetrieve(OrgType.F_DESCRIPTION, u.getDescription(), (b, v) -> b.setDescription(v))
                .nullSafeWithRetrieve(OrgType.F_SUBTYPE, u.getSubtype(), (b, v) -> b.addAllSubtype(v))
                .nullSafeWithRetrieve(OrgType.F_LIFECYCLE_STATE, u.getLifecycleState(), (b, v) -> b.setLifecycleState(v))
                .nullSafeWithRetrieve(OrgType.F_PARENT_ORG_REF, u.getParentOrgRef(), (b, v) -> {
                    return b.addAllParentOrgRef(v.stream().map(a -> {
                        return toReferenceMessage(a, null);
                    }).collect(Collectors.toList()));
                })
                // AssignmentHolderType
                .nullSafeWithRetrieve(OrgType.F_ASSIGNMENT, u.getAssignment(), (b, v) -> {
                    return b.addAllAssignment(v.stream().map(a -> {
                        return AssignmentMessage.newBuilder()
                                .setTargetRef(
                                        toReferenceMessage(a.getTargetRef(), null)
                                ).build();
                    }).collect(Collectors.toList()));
                })
                .nullSafeWithRetrieve(OrgType.F_ARCHETYPE_REF, u.getArchetypeRef(), (b, v) -> {
                    return b.addAllArchetypeRef(v.stream().map(a -> {
                        return toReferenceMessage(a, null);
                    }).collect(Collectors.toList()));
                })
                .nullSafeWithRetrieve(OrgType.F_ROLE_MEMBERSHIP_REF, u.getRoleMembershipRef(), (b, v) -> {
                    return b.addAllRoleMembershipRef(v.stream().map(a -> {
                        return toReferenceMessage(a, null);
                    }).collect(Collectors.toList()));
                })
                // FocusType
                .nullSafeWithRetrieve(OrgType.F_JPEG_PHOTO, toBytesMessage(u.getJpegPhoto()), (b, v) -> b.setJpegPhoto(v))
                .nullSafeWithRetrieve(OrgType.F_COST_CENTER, u.getCostCenter(), (b, v) -> b.setCostCenter(v))
                .nullSafeWithRetrieve(OrgType.F_LOCALITY, toPolyStringMessage(u.getLocality()), (b, v) -> b.setLocality(v))
                .nullSafeWithRetrieve(OrgType.F_PREFERRED_LANGUAGE, u.getPreferredLanguage(), (b, v) -> b.setPreferredLanguage(v))
                .nullSafeWithRetrieve(OrgType.F_LOCALE, u.getLocale(), (b, v) -> b.setLocale(v))
                .nullSafeWithRetrieve(OrgType.F_TIMEZONE, u.getTimezone(), (b, v) -> b.setTimezone(v))
                .nullSafeWithRetrieve(OrgType.F_EMAIL_ADDRESS, u.getEmailAddress(), (b, v) -> b.setEmailAddress(v))
                .nullSafeWithRetrieve(OrgType.F_TELEPHONE_NUMBER, u.getTelephoneNumber(), (b, v) -> b.setTelephoneNumber(v))
                // AbstractRoleType
                .nullSafeWithRetrieve(OrgType.F_DISPLAY_NAME, toPolyStringMessage(u.getDisplayName()), (b, v) -> b.setDisplayName(v))
                .nullSafeWithRetrieve(OrgType.F_IDENTIFIER, u.getIdentifier(), (b, v) -> b.setIdentifier(v))
                .nullSafeWithRetrieve(OrgType.F_REQUESTABLE, u.isRequestable(), (b, v) -> b.setRequestable(v))
                .nullSafeWithRetrieve(OrgType.F_DELEGABLE, u.isDelegable(), (b, v) -> b.setDelegable(v))
                .nullSafeWithRetrieve(OrgType.F_RISK_LEVEL, u.getRiskLevel(), (b, v) -> b.setRiskLevel(v))
                // OrgType
                .nullSafeWithRetrieve(OrgType.F_TENANT, u.isTenant(), (b, v) -> b.setTenant(v))
                .nullSafeWithRetrieve(OrgType.F_MAIL_DOMAIN, u.getMailDomain(), (b, v) -> b.addAllMailDomain(v))
                .nullSafeWithRetrieve(OrgType.F_DISPLAY_ORDER, u.getDisplayOrder(), (b, v) -> b.setDisplayOrder(v))
                // Extension
                .nullSafeWithRetrieve(OrgType.F_EXTENSION, u.getExtension(),
                        (v, ops, hasInclude) -> toItemMessageMap(v, ops, hasInclude),
                        (b, v) -> b.putAllExtension(v))
                .unwrap()
                .build();
    }

    public static ServiceTypeMessage toServiceTypeMessage(ServiceType u, Collection<SelectorOptions<GetOperationOptions>> options) {
        return BuilderWrapper.wrap(ServiceTypeMessage.newBuilder())
                // ObjectType
                .nullSafe(u.getOid(), (b, v) -> b.setOid(v))
                .nullSafe(u.getVersion(), (b, v) -> b.setVersion(v))
                .nullSafe(toPolyStringMessage(u.getName()), (b, v) -> b.setName(v))
                .selectOptions(options)
                .nullSafeWithRetrieve(ServiceType.F_DESCRIPTION, u.getDescription(), (b, v) -> b.setDescription(v))
                .nullSafeWithRetrieve(ServiceType.F_SUBTYPE, u.getSubtype(), (b, v) -> b.addAllSubtype(v))
                .nullSafeWithRetrieve(ServiceType.F_LIFECYCLE_STATE, u.getLifecycleState(), (b, v) -> b.setLifecycleState(v))
                .nullSafeWithRetrieve(ServiceType.F_PARENT_ORG_REF, u.getParentOrgRef(), (b, v) -> {
                    return b.addAllParentOrgRef(v.stream().map(a -> {
                        return toReferenceMessage(a, null);
                    }).collect(Collectors.toList()));
                })
                // AssignmentHolderType
                .nullSafeWithRetrieve(ServiceType.F_ASSIGNMENT, u.getAssignment(), (b, v) -> {
                    return b.addAllAssignment(v.stream().map(a -> {
                        return AssignmentMessage.newBuilder()
                                .setTargetRef(
                                        toReferenceMessage(a.getTargetRef(), null)
                                ).build();
                    }).collect(Collectors.toList()));
                })
                .nullSafeWithRetrieve(ServiceType.F_ARCHETYPE_REF, u.getArchetypeRef(), (b, v) -> {
                    return b.addAllArchetypeRef(v.stream().map(a -> {
                        return toReferenceMessage(a, null);
                    }).collect(Collectors.toList()));
                })
                .nullSafeWithRetrieve(ServiceType.F_ROLE_MEMBERSHIP_REF, u.getRoleMembershipRef(), (b, v) -> {
                    return b.addAllRoleMembershipRef(v.stream().map(a -> {
                        return toReferenceMessage(a, null);
                    }).collect(Collectors.toList()));
                })
                // FocusType
                .nullSafeWithRetrieve(ServiceType.F_JPEG_PHOTO, toBytesMessage(u.getJpegPhoto()), (b, v) -> b.setJpegPhoto(v))
                .nullSafeWithRetrieve(ServiceType.F_COST_CENTER, u.getCostCenter(), (b, v) -> b.setCostCenter(v))
                .nullSafeWithRetrieve(ServiceType.F_LOCALITY, toPolyStringMessage(u.getLocality()), (b, v) -> b.setLocality(v))
                .nullSafeWithRetrieve(ServiceType.F_PREFERRED_LANGUAGE, u.getPreferredLanguage(), (b, v) -> b.setPreferredLanguage(v))
                .nullSafeWithRetrieve(ServiceType.F_LOCALE, u.getLocale(), (b, v) -> b.setLocale(v))
                .nullSafeWithRetrieve(ServiceType.F_TIMEZONE, u.getTimezone(), (b, v) -> b.setTimezone(v))
                .nullSafeWithRetrieve(ServiceType.F_EMAIL_ADDRESS, u.getEmailAddress(), (b, v) -> b.setEmailAddress(v))
                .nullSafeWithRetrieve(ServiceType.F_TELEPHONE_NUMBER, u.getTelephoneNumber(), (b, v) -> b.setTelephoneNumber(v))
                // AbstractRoleType
                .nullSafeWithRetrieve(ServiceType.F_DISPLAY_NAME, toPolyStringMessage(u.getDisplayName()), (b, v) -> b.setDisplayName(v))
                .nullSafeWithRetrieve(ServiceType.F_IDENTIFIER, u.getIdentifier(), (b, v) -> b.setIdentifier(v))
                .nullSafeWithRetrieve(ServiceType.F_REQUESTABLE, u.isRequestable(), (b, v) -> b.setRequestable(v))
                .nullSafeWithRetrieve(ServiceType.F_DELEGABLE, u.isDelegable(), (b, v) -> b.setDelegable(v))
                .nullSafeWithRetrieve(ServiceType.F_RISK_LEVEL, u.getRiskLevel(), (b, v) -> b.setRiskLevel(v))
                // ServiceType
                .nullSafeWithRetrieve(ServiceType.F_DISPLAY_ORDER, u.getDisplayOrder(), (b, v) -> b.setDisplayOrder(v))
                .nullSafeWithRetrieve(ServiceType.F_URL, u.getUrl(), (b, v) -> b.setUrl(v))
                // Extension
                .nullSafeWithRetrieve(ServiceType.F_EXTENSION, u.getExtension(),
                        (v, ops, hasInclude) -> toItemMessageMap(v, ops, hasInclude),
                        (b, v) -> b.putAllExtension(v))
                .unwrap()
                .build();
    }

    public static LookupTableMessage toLookupTableMessage(LookupTableType lookupTable, Collection<SelectorOptions<GetOperationOptions>> options) {
        return BuilderWrapper.wrap(LookupTableMessage.newBuilder())
                .nullSafe(lookupTable.getOid(), (b, v) -> b.setOid(v))
                .nullSafe(lookupTable.getVersion(), (b, v) -> b.setVersion(v))
                .nullSafe(toPolyStringMessage(lookupTable.getName()), (b, v) -> b.setName(v))
                .selectOptions(options)
                .nullSafeWithRetrieve(LookupTableType.F_DESCRIPTION, lookupTable.getDescription(), (b, v) -> b.setDescription(v))
                .nullSafeWithRetrieve(LookupTableType.F_ROW, lookupTable.getRow(), (b, v) -> {
                    return b.addAllRow(v.stream().map(a -> {
                        return toLookupTableRowMessage(a);
                    }).collect(Collectors.toList()));
                })
                .unwrap()
                .build();
    }

    private static LookupTableRowMessage toLookupTableRowMessage(LookupTableRowType row) {
        return BuilderWrapper.wrap(LookupTableRowMessage.newBuilder())
                .nullSafe(row.getId(), (b, v) -> b.setId(v))
                .nullSafe(row.getKey(), (b, v) -> b.setKey(v))
                .nullSafe(row.getValue(), (b, v) -> b.setValue(v))
                .nullSafe(toPolyStringMessage(row.getLabel()), (b, v) -> b.setLabel(v))
                .nullSafe(row.getLastChangeTimestamp(), (b, v) -> b.setLastChangeTimestamp(toTimestamp(v)))
                .unwrap()
                .build();
    }

    public static Timestamp toTimestamp(XMLGregorianCalendar timestamp) {
        long millis = timestamp.toGregorianCalendar().getTimeInMillis();
        return Timestamp.newBuilder()
                .setSeconds(millis / 1000)
                .setNanos((int) ((millis % 1000) * 1000000))
                .build();
    }

    public static Map<String, ItemMessage> toItemMessageMap(ExtensionType extension,
                                                            Collection<SelectorOptions<GetOperationOptions>> options,
                                                            boolean hasInclude) {
        if (extension == null) {
            return null;
        }

        Map<String, ItemMessage> map = new LinkedHashMap<>();

        PrismContainerValue<?> extensionValue = extension.asPrismContainerValue();
        for (Item item : extensionValue.getItems()) {
            ItemDefinition definition = item.getDefinition();

            if (definition == null) {
                LOGGER.warn("No schema for the extension value: {}", item);
                continue;
            }

            // Currently, it doesn't use namespaceURI as the key
            String key = definition.getItemName().getLocalPart();

            if (!SelectorOptions.hasToIncludePath(item.getPath(), options, !hasInclude)) {
                continue;
            }

            try {
                map.put(key, toItemMessage(definition, item));
            } catch (SchemaException e) {
                StatusRuntimeException exception = Status.INVALID_ARGUMENT
                        .withDescription("invalid_schema: " + e.getMessage())
                        .withCause(e)
                        .asRuntimeException();
                throw exception;
            }
        }

        return map;
    }

    private static PrismContainerValueMessage toPrismContainerValueMessage(PrismContainerDefinition definition,
                                                                           PrismContainerValue<?> value,
                                                                           Collection<SelectorOptions<GetOperationOptions>> options,
                                                                           boolean hasInclude) throws SchemaException {
        PrismContainerValueMessage.Builder builder = PrismContainerValueMessage.newBuilder();

        if (value.getId() != null) {
            builder.setId(value.getId());
        }

        for (Item<?, ?> item : value.getItems()) {
            if (!SelectorOptions.hasToIncludePath(item.getPath(), options, !hasInclude)) {
                continue;
            }

            ItemDefinition itemDefinition = item.getDefinition();
            String key = itemDefinition.getItemName().getLocalPart();
            builder.putValue(key, toItemMessage(itemDefinition, item, options, hasInclude));
        }

        return builder.build();
    }

    public static PrismObjectMessage toPrismObjectMessage(ItemDefinition definition, PrismObject<? extends ObjectType> prismObject) throws SchemaException {
        return toPrismObjectMessage(definition, prismObject, null, false);
    }

    public static PrismObjectMessage toPrismObjectMessage(ItemDefinition definition, PrismObject<? extends ObjectType> prismObject,
                                                          Collection<SelectorOptions<GetOperationOptions>> options, boolean hasInclude) throws SchemaException {
        if (definition.isSingleValue()) {
            if (prismObject.size() > 1) {
                throw new SchemaException("It must be single value: " + definition);
            }
        }

        PrismObjectMessage.Builder builder = PrismObjectMessage.newBuilder()
//                .setItemName(toQNameMessage(definition.getItemName()))
//                .setTypeName(toQNameMessage(definition.getTypeName()))
                .setOid(prismObject.getOid());

        for (Item item : prismObject.getValue().getItems()) {
            if (!SelectorOptions.hasToIncludePath(item.getPath(), options, !hasInclude)) {
                continue;
            }
            builder.putValue(item.getElementName().getLocalPart(), toItemMessage(item.getDefinition(), item, options, hasInclude));
        }

        return builder.build();
    }

    public static ItemMessage toItemMessage(ItemDefinition definition, Item<?, ?> item) throws SchemaException {
        return toItemMessage(definition, item, null, false);
    }

    public static ItemMessage toItemMessage(ItemDefinition definition, Item<?, ?> item,
                                            Collection<SelectorOptions<GetOperationOptions>> options, boolean hasInclude) throws SchemaException {
        if (definition.isSingleValue()) {
            if (item.size() > 1) {
                throw new SchemaException("It must be single value: " + definition);
            }
        }

        ItemMessage.Builder builder = ItemMessage.newBuilder()
//                .setItemName(toQNameMessage(definition.getItemName()))
//                .setTypeName(toQNameMessage(definition.getTypeName()))
                .setMultiple(definition.isMultiValue());

        if (definition instanceof PrismPropertyDefinition) {
            return builder
                    .setProperty(toPrismPropertyMessage((PrismPropertyDefinition) definition, (PrismProperty) item))
                    .build();

        } else if (definition instanceof PrismContainerDefinition) {
            return builder
                    .setContainer(toPrismContainerMessage((PrismContainerDefinition) definition, (PrismContainer) item, options, hasInclude))
                    .build();

        } else if (definition instanceof PrismReferenceDefinition) {
            return builder
                    .setRef(toPrismReferenceMessage((PrismReferenceDefinition) definition, (PrismReference) item))
                    .build();

        } else {
            throw new SchemaException(definition.getClass() + " is not supported");
        }
    }

    private static PrismPropertyMessage toPrismPropertyMessage(PrismPropertyDefinition<?> definition, PrismProperty<?> value) {
        if (definition.isSingleValue()) {
            return PrismPropertyMessage.newBuilder()
                    .addValues(toPrismPropertyValueMessage(definition, value.getValue()))
                    .build();
        } else {
            PrismPropertyMessage.Builder builder = PrismPropertyMessage.newBuilder();
            for (PrismPropertyValue<?> v : value.getValues()) {
                builder.addValues(toPrismPropertyValueMessage(definition, v));
            }
            return builder.build();
        }
    }

    private static PrismContainerMessage toPrismContainerMessage(PrismContainerDefinition definition, PrismContainer value,
                                                                 Collection<SelectorOptions<GetOperationOptions>> options, boolean hasInclude) throws SchemaException {
        if (definition.isSingleValue()) {
            return PrismContainerMessage.newBuilder()
                    .addValues(toPrismContainerValueMessage(definition, value.getValue(), options, hasInclude))
                    .build();
        } else {
            PrismContainerMessage.Builder builder = PrismContainerMessage.newBuilder();
            for (Object v : value.getValues()) {
                builder.addValues(toPrismContainerValueMessage(definition, (PrismContainerValue) v, options, hasInclude));
            }
            return builder.build();
        }
    }

    private static PrismReferenceMessage toPrismReferenceMessage(PrismReferenceDefinition definition, PrismReference value) {
        if (definition.isSingleValue()) {
            return PrismReferenceMessage.newBuilder()
                    .addValues(toReferenceMessage(definition, value.getValue()))
                    .build();
        } else {
            PrismReferenceMessage.Builder builder = PrismReferenceMessage.newBuilder();
            for (PrismReferenceValue v : value.getValues()) {
                ReferenceMessage message = toReferenceMessage(definition, v);
                if (message == null) {
                    continue;
                }
                builder.addValues(message);
            }
            return builder.build();
        }
    }

    private static PrismPropertyValueMessage toPrismPropertyValueMessage(PrismPropertyDefinition definition, PrismPropertyValue<?> value) {
        PrismPropertyValueMessage.Builder builder = PrismPropertyValueMessage.newBuilder();
        if (definition.getTypeClass() == String.class) {
            // TODO need type check?
            builder.setString((String) value.getValue());
        } else if (definition.getTypeClass() == PolyString.class) {
            // TODO need type check?
            builder.setPolyString(toPolyStringMessage((PolyString) value.getValue()));
        } else if (definition.getTypeClass() == int.class) {
            builder.setInteger(toIntegerMessage((Integer) value.getValue()));
        } else if (definition.getTypeClass() == long.class) {
            builder.setLong(toLongMessage((Long) value.getValue()));
        } else if (definition.getTypeName().equals(ProtectedStringType.COMPLEX_TYPE)) {
            Protector protector = definition.getPrismContext().getDefaultProtector();
            ProtectedStringType p = (ProtectedStringType) value.getValue();
            try {
                builder.setString(protector.decryptString(p));
            } catch (EncryptionException e) {
                StatusRuntimeException exception = Status.INTERNAL
                        .withDescription("invalid_schema: Can't decrypt " + definition.getItemName())
                        .asRuntimeException();
                throw exception;
            }
        } else {
            builder.setString(value.getRealValue().toString());
        }
        return builder.build();
    }

    public static ItemPath toItemPathValue(ItemPathMessage itemPath) {
        List<QName> qnames = itemPath.getPathList().stream()
                .map(x -> toQNameValue(x))
                .collect(Collectors.toList());
        return ItemPath.create(qnames);
    }

    public static List<PrismValue> toPrismValueList(PrismContext prismContext, ItemDefinition definition, List<PrismValueMessage> prismValue) throws SchemaException {
        List<PrismValue> values = new ArrayList<>();
        for (PrismValueMessage pvm : prismValue) {
            values.add(toPrismValue(prismContext, definition, pvm));
        }
        return values;
    }

    private static PrismValue toPrismValue(PrismContext prismContext, ItemDefinition definition, PrismValueMessage prismValueMessage) throws SchemaException {
        if (prismValueMessage.hasContainer()) {
            if (!(definition instanceof PrismContainerDefinition)) {
                StatusRuntimeException exception = Status.INVALID_ARGUMENT
                        .withDescription("invalid_prism_container_schema")
                        .asRuntimeException();
                throw exception;
            }
            return toPrismContainerValue(prismContext, (PrismContainerDefinition) definition, prismValueMessage.getContainer());

        } else if (prismValueMessage.hasProperty()) {
            if (!(definition instanceof PrismPropertyDefinition)) {
                StatusRuntimeException exception = Status.INVALID_ARGUMENT
                        .withDescription("invalid_prism_property_schema")
                        .asRuntimeException();
                throw exception;
            }
            return toPrismPropertyValue(prismContext, (PrismPropertyDefinition) definition, prismValueMessage.getProperty());

        } else {
            // Reference case
            if (!(definition instanceof PrismReferenceDefinition)) {
                StatusRuntimeException exception = Status.INVALID_ARGUMENT
                        .withDescription("invalid_prism_reference_schema")
                        .asRuntimeException();
                throw exception;
            }
            return toPrismReferenceValue(prismContext, (PrismReferenceDefinition) definition, prismValueMessage.getRef());
        }
    }

    private static PrismContainer toPrismContainer(PrismContext prismContext, PrismContainerDefinition definition,
                                                   PrismContainerMessage container) throws SchemaException {
        PrismContainer pc = prismContext.itemFactory().createContainer(definition.getItemName(), definition);
        for (PrismContainerValueMessage pcvm : container.getValuesList()) {
            pc.add(toPrismContainerValue(prismContext, definition, pcvm));
        }
        return pc;
    }

    private static PrismContainerValue toPrismContainerValue(PrismContext prismContext, PrismContainerDefinition definition,
                                                             PrismContainerValueMessage message) throws SchemaException {
        PrismContainerValue impl = prismContext.itemFactory().createContainerValue();
        // Need to apply here
        impl.applyDefinition(definition);
        if (message.getId() != 0) {
            impl.setId(message.getId());
        }

        for (Map.Entry<String, ItemMessage> entry : message.getValueMap().entrySet()) {
            String k = entry.getKey();
            ItemMessage v = entry.getValue();

            QName qname = toQName(v, k);
            ItemPath path = ItemPath.create(qname);

            ItemDefinition itemDefinition = definition.findItemDefinition(path);
            if (itemDefinition == null) {
                throw new SchemaException("No schema: " + qname);
            }

            if (itemDefinition instanceof PrismPropertyDefinition) {
                impl.add(toPrismProperty(prismContext, (PrismPropertyDefinition) itemDefinition, v.getProperty()));

            } else if (itemDefinition instanceof PrismContainerDefinition) {
                impl.add(toPrismContainer(prismContext, (PrismContainerDefinition) itemDefinition, v.getContainer()));

            } else if (itemDefinition instanceof PrismReferenceDefinition) {
                impl.add(toPrismReference(prismContext, (PrismReferenceDefinition) itemDefinition, v.getRef()));

            } else {
                throw new UnsupportedOperationException(itemDefinition.getClass() + " is not supported");
            }
        }

        return impl;
    }

    private static PrismObjectValue toPrismObjectValue(PrismContext prismContext, PrismContainerDefinition definition,
                                                       PrismContainerValueMessage message, Objectable objectable) throws SchemaException {
        PrismObjectValue impl = prismContext.itemFactory().createObjectValue(objectable);
        // Need to apply here
        impl.applyDefinition(definition);

        for (Map.Entry<String, ItemMessage> entry : message.getValueMap().entrySet()) {
            String k = entry.getKey();
            ItemMessage v = entry.getValue();

            QName qname = toQName(v, k);
            ItemPath path = ItemPath.create(qname);

            ItemDefinition itemDefinition = definition.findItemDefinition(path);
            if (itemDefinition == null) {
                throw new SchemaException("No schema: " + qname);
            }

            if (itemDefinition instanceof PrismPropertyDefinition) {
                impl.add(toPrismProperty(prismContext, (PrismPropertyDefinition) itemDefinition, v.getProperty()));

            } else if (itemDefinition instanceof PrismContainerDefinition) {
                impl.add(toPrismContainer(prismContext, (PrismContainerDefinition) itemDefinition, v.getContainer()));

            } else if (itemDefinition instanceof PrismReferenceDefinition) {
                impl.add(toPrismReference(prismContext, (PrismReferenceDefinition) itemDefinition, v.getRef()));

            } else {
                throw new UnsupportedOperationException(itemDefinition.getClass() + " is not supported");
            }
        }

        return impl;
    }

    private static Item toPrismReference(PrismContext prismContext, PrismReferenceDefinition definition, PrismReferenceMessage ref) throws SchemaException {
        PrismReference pp = prismContext.itemFactory().createReference(definition.getItemName(), definition);
        for (ReferenceMessage value : ref.getValuesList()) {
            pp.add(toPrismReferenceValue(prismContext, definition, value));
        }
        return pp;
    }

    private static Item toPrismProperty(PrismContext prismContext, PrismPropertyDefinition definition, PrismPropertyMessage property) {
        PrismProperty pp = prismContext.itemFactory().createProperty(definition.getItemName(), definition);
        for (PrismPropertyValueMessage ppvm : property.getValuesList()) {
            pp.addValue(toPrismPropertyValue(prismContext, definition, ppvm));
        }
        return pp;
    }

    private static PrismPropertyValue toPrismPropertyValue(PrismContext prismContext, PrismPropertyDefinition definition, PrismPropertyValueMessage property) {
        return new PrismPropertyValueImpl(toRealValue(prismContext, definition, property));
    }

    private static Object toRealValue(PrismContext prismContext, PrismPropertyDefinition definition, PrismPropertyValueMessage message) {
        if (message.hasInteger()) {
            return toIntegerValue(message.getInteger());
        }
        if (message.hasLong()) {
            return toLongValue(message.getLong());
        }
        if (message.hasPolyString()) {
            // Need to return PolyString in PrismValue
            return toPolyString(message.getPolyString());
        }
        // string case
        String s = message.getString();

        // ProtectedStringType case
        if (definition.getTypeName().equals(ProtectedStringType.COMPLEX_TYPE)) {
            ProtectedStringType p = new ProtectedStringType();
            p.setClearValue(s);
            return p;
        }

        return s;
    }

    private static PolyString toPolyString(PolyStringMessage message) {
        if (isEmpty(message)) {
            return null;
        }
        return new PolyString(message.getOrig(), message.getNorm());
    }

    private static PrismReferenceValue toPrismReferenceValue(PrismContext prismContext, PrismReferenceDefinition definition, ReferenceMessage ref) throws SchemaException {
        return toObjectReferenceTypeValue(prismContext, ref).asReferenceValue();
    }
}
