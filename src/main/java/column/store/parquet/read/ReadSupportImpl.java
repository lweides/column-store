package column.store.parquet.read;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

class ReadSupportImpl extends ReadSupport<Object> {

  private final Map<String, BaseReader> readers;
  private MessageType readSchema;

  ReadSupportImpl(final Map<String, BaseReader> readers) {
    this.readers = readers;
  }

  @Override
  public RecordMaterializer<Object> prepareForRead(
          final Configuration configuration,
          final Map keyValueMetaData,
          final MessageType fileSchema,
          final ReadContext readContext) {
    return new RecordMaterializerImpl(readSchema, readers);
  }

  @Override
  public ReadContext init(final InitContext context) {
    var configuredSchema = context.getConfiguration().get(PARQUET_READ_SCHEMA);
    readSchema = configuredSchema == null ? context.getFileSchema() : MessageTypeParser.parseMessageType(configuredSchema);

    for (var column : readSchema.getColumns()) {
      var name = column.getPrimitiveType().getName();
      switch (column.getPrimitiveType().getPrimitiveTypeName()) {
        case INT64 -> readers.computeIfAbsent(name, n -> new LongReader());
        case BOOLEAN -> readers.computeIfAbsent(name, n -> new BooleanReader());
        case DOUBLE -> readers.computeIfAbsent(name, n -> new DoubleReader());
        case BINARY -> {
          boolean isString = name.endsWith("-string");
          if (isString) {
            readers.computeIfAbsent(name, n -> new StringReader());
          } else {
            readers.computeIfAbsent(name, n -> new IdReader());
          }
        }
        default -> throw new IllegalStateException("Unknown type: " + column.getPrimitiveType());
      }
    }

    return new ReadContext(readSchema);
  }
}
