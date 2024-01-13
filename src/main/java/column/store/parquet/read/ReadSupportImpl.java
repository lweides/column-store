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
    readSchema = MessageTypeParser.parseMessageType(context.getConfiguration().get(PARQUET_READ_SCHEMA));
    return new ReadContext(readSchema);
  }
}
