package column.store.input;

import column.store.api.column.Column;
import column.store.api.query.Query;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.file.Path;

public class InputBench {

    @SuppressWarnings("checkstyle:VisibilityModifier")
    @State(Scope.Thread)
    public static class BenchState {

        public InMemoryReader reader;
        public InMemoryReader.StringByteReader nameReader;
        public InMemoryReader.BooleanByteReader maleReader;
        public InMemoryReader.LongByteReader ageReader;
        public InMemoryReader.DoubleByteReader heightReader;
        public InMemoryReader.IdByteReader idReader;

        @Setup(Level.Iteration)
        public void setup() {
            reader = new InMemoryReader();
            var name = Column.forString("name");
            var male = Column.forBoolean("male");
            var age = Column.forLong("age");
            var height = Column.forDouble("height");
            var id = Column.forId("id");
            var path = Path.of("testDir");
            var query = Query.from(path).select(age, height, male, name, id).allOf();
            reader.query(query);
            nameReader = reader.of(name);
            maleReader = reader.of(male);
            ageReader = reader.of(age);
            heightReader = reader.of(height);
            idReader = reader.of(id);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void readRecords(final Blackhole blackhole, final BenchState state) {
        state.reader.reset();
        while (state.reader.hasNext()) {
            state.reader.next();
            if (state.nameReader.isPresent()) {
                blackhole.consume(state.nameReader.get());
            }
            if (state.maleReader.isPresent()) {
                blackhole.consume(state.maleReader.get());
            }
            if (state.ageReader.isPresent()) {
                blackhole.consume(state.ageReader.get());
            }
            if (state.heightReader.isPresent()) {
                blackhole.consume(state.heightReader.get());
            }
            if (state.idReader.isPresent()) {
                blackhole.consume(state.idReader.get());
            }
        }
    }
}
