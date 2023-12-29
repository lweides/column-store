package column.store.input;

import column.store.api.column.Column;
import column.store.inmemory.InMemoryReader;
import column.store.inmemory.InMemoryWriter;

import com.devskiller.jfairy.Fairy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

/**
 * Creation of a file for {@link InMemoryReader}.
 */
@SuppressWarnings("checkstyle:MagicNumber")

public final class CreateInput {

    private static final int NUMBER_OF_RECORDS = 1_000_000;
    private static final Random RANDOM = new Random();

    static {
        long seed = RANDOM.nextLong();
        System.out.println("Using seed: " + seed);
        RANDOM.setSeed(seed);
    }

    public static void main(final String[] args) throws IOException {
        var name = Column.forString("name");
        var male = Column.forBoolean("male");
        var age = Column.forLong("age");
        var height = Column.forDouble("height");
        var id = Column.forId("id");

        var testDir = Path.of("testDir");
        deleteRecursively(testDir);

        try (var writer = new InMemoryWriter(testDir)) {
            var nameWriter = writer.of(name);
            var maleWriter = writer.of(male);
            var ageWriter = writer.of(age);
            var heightWriter = writer.of(height);
            var idWriter = writer.of(id);

            var fairy = Fairy.builder().withRandomSeed(RANDOM.nextInt()).build();

            for (int i = 0; i < NUMBER_OF_RECORDS; i++) {
                var person = fairy.person();

                nameWriter.write(person.getFullName());
                if (hasProperty()) {
                    ageWriter.write(person.getAge());
                } else {
                    ageWriter.writeNull();
                }
                if (hasProperty()) {
                    heightWriter.write(person.getAge() * 1.76); // no double or float by fairy
                } else {
                    heightWriter.writeNull();
                }
                if (hasProperty()) {
                    maleWriter.write(person.isMale());
                } else {
                    maleWriter.writeNull();
                }
                if (hasProperty()) {
                    idWriter.write(person.getDateOfBirth().toString().getBytes(StandardCharsets.UTF_8)); // random byte[]
                } else {
                    idWriter.writeNull();
                }

                writer.next();
            }
        }
    }

    private static void deleteRecursively(final Path path) {
        if (Files.isDirectory(path)) {
            try {
                try (var children = Files.list(path)) {
                    children.forEach(CreateInput::deleteRecursively);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean hasProperty() {
        return RANDOM.nextDouble() < 0.8;
    }

    private CreateInput() {
        // hidden util constructor
    }
}
