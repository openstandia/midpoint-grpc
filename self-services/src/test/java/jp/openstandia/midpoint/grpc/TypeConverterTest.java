package jp.openstandia.midpoint.grpc;

import com.evolveum.midpoint.prism.PrismService;
import com.evolveum.midpoint.prism.impl.PrismContextImpl;
import com.evolveum.midpoint.prism.impl.schema.SchemaRegistryImpl;
import com.evolveum.midpoint.prism.path.ItemName;
import com.evolveum.midpoint.prism.polystring.PolyString;
import com.evolveum.midpoint.xml.ns._public.common.common_3.UserType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TypeConverterTest {

    @BeforeEach
    void before() {
        PrismContextImpl prismContext = PrismContextImpl.create(new SchemaRegistryImpl());
        PrismService.get().prismContext(prismContext);
    }

    @Test
    public void toItemName() {
        ItemName name = TypeConverter.toItemName(DefaultUserTypePath.F_NAME);
        assertEquals(UserType.F_NAME, name, "Should get ItemName of ObjectType");

        ItemName locality = TypeConverter.toItemName(DefaultUserTypePath.F_LOCALITY);
        assertEquals(UserType.F_LOCALITY, locality, "Should get ItemName of FocusType");

        ItemName fullName = TypeConverter.toItemName(DefaultUserTypePath.F_FULL_NAME);
        assertEquals(UserType.F_FULL_NAME, fullName, "Should get ItemName of UserType");
    }

    @Test
    public void toRealValueStringClass() {
        Object foo = TypeConverter.toRealValue("foo", String.class);
        assertEquals(String.class, foo.getClass());

        Object hoge = TypeConverter.toRealValue("hoge", PolyString.class);
        assertEquals(PolyString.class, hoge.getClass());
    }
}
