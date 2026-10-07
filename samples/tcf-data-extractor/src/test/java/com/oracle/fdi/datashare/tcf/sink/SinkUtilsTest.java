package com.oracle.fdi.datashare.tcf.sink;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import com.oracle.fdi.datashare.tcf.common.vo.DatasetSchema;
import java.util.List;
import org.junit.Test;
public class SinkUtilsTest {
    @Test
    public void usesCompleteDateEffectiveColumnsForDeletes() {
        DatasetSchema schema = schema("ID", "end_date, start_date");
        List<String> result = SinkUtils.getDeleteColumns(
                schema, new String[] {"id", "end_date", "start_date"});
        assertEquals(List.of("START_DATE", "END_DATE"), result);
    }
    @Test
    public void fallsBackToPrimaryKeyWhenDateEffectiveColumnsAreIncomplete() {
        DatasetSchema schema = schema("ID", "START_DATE, END_DATE");
        List<String> result = SinkUtils.getDeleteColumns(
                schema, new String[] {"id", "start_date"});
        assertEquals(List.of("ID"), result);
    }
    @Test
    public void preservesPrimaryKeyDeleteBehaviorForExistingSchemas() {
        DatasetSchema schema = schema("ID", null);
        List<String> result = SinkUtils.getDeleteColumns(
                schema, new String[] {"name", "id"});
        assertEquals(List.of("ID"), result);
    }
    @Test
    public void usesAvailableSchemaColumnsWhenNoConfiguredKeyIsComplete() {
        DatasetSchema schema = schema("ID, NAME", "START_DATE, END_DATE");
        List<String> result = SinkUtils.getDeleteColumns(
                schema, new String[] {"name", "end_date", "unknown"});
        assertEquals(List.of("END_DATE", "NAME"), result);
    }
    @Test
    public void failsWhenNoDeleteColumnsMatchThePublishedSchema() {
        DatasetSchema schema = schema("ID", "START_DATE");
        assertThrows(IllegalArgumentException.class,
                () -> SinkUtils.getDeleteColumns(schema, new String[] {"unknown"}));
    }
    private DatasetSchema schema(String primaryKeyCols, String dateEffectiveCols) {
        DatasetSchema schema = new DatasetSchema();
        schema.setPrimaryKeyCols(primaryKeyCols);
        schema.setDateEffectiveCols(dateEffectiveCols);
        schema.setSchema(List.of(
                column("ID"),
                column("START_DATE"),
                column("END_DATE"),
                column("NAME")));
        return schema;
    }
    private DatasetSchema.Column column(String name) {
        DatasetSchema.Column column = new DatasetSchema.Column();
        column.setColName(name);
        return column;
    }
}
