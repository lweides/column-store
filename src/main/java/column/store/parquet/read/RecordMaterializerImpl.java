package column.store.parquet.read;

import java.util.List;
import java.util.Map;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.TypeConverter;

class RecordMaterializerImpl extends RecordMaterializer<Object> {

  /** {@link #getCurrentRecord()} needs to return something which {@code != null} */
  private final Object result = new Object();
  private final Converter root;
  private final Map<String, BaseReader> readers;

  RecordMaterializerImpl(final MessageType schema, final Map<String, BaseReader> readers) {
    this.readers = readers;
    this.root = schema.convertWith(new TypeConverter<>() {

      @Override
      public Converter convertPrimitiveType(final List<GroupType> path, final PrimitiveType primitiveType) {
        return readers.get(primitiveType.getName());
      }

      @Override
      public Converter convertGroupType(final List<GroupType> path, final GroupType groupType, final List<Converter> children) {
        // we only support primitives, no need for groups
        return new GroupConverter() {
          public Converter getConverter(final int fieldIndex) {
            return children.get(fieldIndex);
          }

          public void start() {
          }

          public void end() {
          }
        };
      }

      @Override
      public Converter convertMessageType(final MessageType messageType, final List<Converter> children) {
        // we only support primitives, no need for messages
        return convertGroupType(null, messageType, children);
      }
    });
  }

  @Override
  public Object getCurrentRecord() {
    return result;
  }

  @Override
  public void skipCurrentRecord() {
    readers.values().forEach(BaseReader::reset);
  }

  @Override
  public GroupConverter getRootConverter() {
    return root.asGroupConverter();
  }
}
