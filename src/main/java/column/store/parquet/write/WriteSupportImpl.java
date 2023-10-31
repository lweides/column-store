package column.store.parquet.write;

import static column.store.util.Conditions.checkState;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageTypeParser;

import column.store.api.column.Column;

class WriteSupportImpl extends WriteSupport<Object> {

  private final BaseWriter[] columnWriters;
  private RecordConsumer recordConsumer;

  WriteSupportImpl(final BaseWriter[] columnWriters) {
    this.columnWriters = columnWriters;
  }

  @Override
  public WriteContext init(final Configuration configuration) {
    var sb = new StringBuilder("message record {");
    for (var columnWriter : columnWriters) {
      sb
              .append("optional ")
              .append(ofColumn(columnWriter.column()))
              .append(" ")
              .append(columnWriter.column().name())
              .append(";");
    }
    sb.append("}");

    return new WriteContext(MessageTypeParser.parseMessageType(sb.toString()), Map.of());
  }

  @Override
  public void prepareForWrite(final RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(final Object record) {
    checkState(recordConsumer != null, "#prepareForWrite(RecordConsumer) has not been called or supplied consumer was null");

    recordConsumer.startMessage();
    for (var column : columnWriters) {
      column.accept(recordConsumer);
    }
    recordConsumer.endMessage();
  }

  private static String ofColumn(final Column column) {
    return switch (column.type()) {
      case BOOLEAN -> "boolean";
      case DOUBLE -> "double";
      case ID, STRING -> "binary";
      case LONG -> "int64";
    };
  }
}
