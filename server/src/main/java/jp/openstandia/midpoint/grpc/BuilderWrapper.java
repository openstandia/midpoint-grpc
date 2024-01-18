package jp.openstandia.midpoint.grpc;

import com.evolveum.midpoint.prism.path.ItemPath;
import com.evolveum.midpoint.schema.GetOperationOptions;
import com.evolveum.midpoint.schema.RetrieveOption;
import com.evolveum.midpoint.schema.SelectorOptions;

import java.util.Collection;
import java.util.Optional;

public class BuilderWrapper<T> {

    public interface Task<T, V> {
        T run(T builer, V value);
    }

    private T b;

    private Collection<SelectorOptions<GetOperationOptions>> options;
    private boolean hasInclude = false;

    private BuilderWrapper(T b) {
        this.b = b;
    }

    public static <T> BuilderWrapper<T> wrap(T b) {
        return new BuilderWrapper<T>(b);
    }

    public <V> BuilderWrapper<T> nullSafe(V value, Task<T, V> t) {
        if (value != null) {
            t.run((T) this.b, value);
        }
        return this;
    }

    public <V> BuilderWrapper<T> selectOptions(Collection<SelectorOptions<GetOperationOptions>> options) {
        if (options != null) {
            this.options = options;
            Optional<RetrieveOption> exclude = options.stream()
                    .map(o -> o.getOptions().getRetrieve())
                    .filter(r -> r == RetrieveOption.INCLUDE)
                    .findFirst();
            if (exclude.isPresent()) {
                this.hasInclude = true;
            }
        }
        return this;
    }

    public <V> BuilderWrapper<T> nullSafeWithRetrieve(ItemPath path, V value, Task<T, V> t) {
        if (!SelectorOptions.hasToIncludePath(path, options, !this.hasInclude)) {
            return this;
        }
        if (value != null) {
            t.run((T) this.b, value);
        }
        return this;
    }

    public <V, O> BuilderWrapper<T> nullSafeWithRetrieve(ItemPath path, V value, Converter<V, O> converter, Task<T, O> t) {
        if (value != null) {
            O converted = converter.run(value, this.options, this.hasInclude);

            if (converted != null) {
                t.run((T) this.b, converted);
            }
        }
        return this;
    }

    public interface Converter<V, O> {
        O run(V value, Collection<SelectorOptions<GetOperationOptions>> options, boolean hasInclude);
    }

    public T unwrap() {
        return b;
    }
}