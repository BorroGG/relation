import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PositionWritable implements WritableComparable<PositionWritable> {

    public Integer positionId;
    public String title;

    @Override
    public int compareTo(PositionWritable o) {
        return this.positionId.compareTo(o.positionId);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(positionId);
        dataOutput.writeUTF(title);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        positionId = dataInput.readInt();
        title = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return positionId + "\t" + title;
    }
}
