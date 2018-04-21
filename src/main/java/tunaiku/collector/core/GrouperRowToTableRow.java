package tunaiku.collector.core;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.List;

public class GrouperRowToTableRow extends DoFn<GrouperRow,TableRow>{
    PCollectionView<List<String>> columnNamesCollection;

    public GrouperRowToTableRow(PCollectionView<List<String>> columnNamesCollection) {
        this.columnNamesCollection = columnNamesCollection;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        GrouperRow data = c.element();
        System.out.println(c.sideInput(columnNamesCollection));
        List<String> columnNames = c.sideInput(columnNamesCollection);
        List<Object> fields = data.fields();
        TableRow row = new TableRow();
        for(int i = 0; i < fields.size(); i++){
            Object fieldData = fields.get(i);
            String columnName = columnNames.get(i);
            if(fieldData == null) continue;
            String sFieldData = fieldData.toString();
            if(!sFieldData.toLowerCase().equals("null")) row.put(columnName, sFieldData);
        }
        c.output(row);
    }
}
