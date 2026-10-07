package com.oracle.fdi.datashare.tcf.sink;
import static org.junit.Assert.assertEquals;
import com.oracle.fdi.datashare.tcf.common.vo.DatasetSchema;
import com.oracle.fdi.datashare.tcf.common.vo.PipelineManifest;
import io.delta.tables.DeltaTable;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
public class DeltaSinkTest {
    private static SparkSession spark;
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @BeforeClass
    public static void setUpSpark() {
        spark = SparkSession.builder()
                .appName("DeltaSinkTest")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();
    }
    @AfterClass
    public static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }
    @Test
    public void deletesByDateEffectiveColumnsWhenPrimaryKeyIsNotPublished() throws Exception {
        String basePath = temporaryFolder.newFolder("date-effective").getAbsolutePath();
        DeltaSink sink = sink(basePath);
        DatasetSchema schema = schema("ID", "START_DATE, END_DATE");
        sink.applyChangeSets("ORDERS", spark.sql("""
                SELECT 1 AS ID, DATE '2026-01-01' AS START_DATE, DATE '2026-12-31' AS END_DATE,
                       'first' AS NAME, 'insert' AS fdi_change_type,
                       'full' AS fdi_table_change_type, 1L AS fdi_scn_id
                UNION ALL
                SELECT 2, DATE '2026-01-01', DATE '2026-12-31', 'second', 'insert', 'full', 1L
                UNION ALL
                SELECT 3, DATE '2027-01-01', DATE '2027-12-31', 'third', 'insert', 'full', 1L
                """), schema);
        sink.applyChangeSets("ORDERS", spark.sql("""
                SELECT DATE '2026-01-01' AS START_DATE, DATE '2026-12-31' AS END_DATE,
                       'delete' AS fdi_change_type, 'incremental' AS fdi_table_change_type,
                       2L AS fdi_scn_id
                """), schema);
        List<Row> remaining = readTable(basePath, "ORDERS").orderBy("ID").collectAsList();
        assertEquals(1, remaining.size());
        assertEquals(3, remaining.get(0).getInt(0));
    }
    @Test
    public void preservesPrimaryKeyDeletesForExistingSchemas() throws Exception {
        String basePath = temporaryFolder.newFolder("primary-key").getAbsolutePath();
        DeltaSink sink = sink(basePath);
        DatasetSchema schema = schema("ID", null);
        sink.applyChangeSets("ORDERS", spark.sql("""
                SELECT 1 AS ID, DATE '2026-01-01' AS START_DATE, DATE '2026-12-31' AS END_DATE,
                       'first' AS NAME, 'insert' AS fdi_change_type,
                       'full' AS fdi_table_change_type, 1L AS fdi_scn_id
                UNION ALL
                SELECT 2, DATE '2026-01-01', DATE '2026-12-31', 'second', 'insert', 'full', 1L
                """), schema);
        sink.applyChangeSets("ORDERS", spark.sql("""
                SELECT 1 AS ID, 'delete' AS fdi_change_type,
                       'incremental' AS fdi_table_change_type, 2L AS fdi_scn_id
                """), schema);
        List<Row> remaining = readTable(basePath, "ORDERS").orderBy("ID").collectAsList();
        assertEquals(1, remaining.size());
        assertEquals(2, remaining.get(0).getInt(0));
    }
    private DeltaSink sink(String basePath) {
        PipelineManifest.SinkConfig config = new PipelineManifest.SinkConfig();
        config.setType("LOCAL_FILESYSTEM");
        config.setFormat("delta_lake");
        config.setConfig(Map.of("path", basePath));
        DeltaSink sink = new DeltaSink(config);
        sink.initialize(spark);
        return sink;
    }
    private Dataset<Row> readTable(String basePath, String datasetName) {
        return DeltaTable.forPath(spark, basePath + "/" + datasetName).toDF();
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
