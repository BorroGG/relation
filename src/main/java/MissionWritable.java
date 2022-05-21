import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MissionWritable implements WritableComparable<MissionWritable> {

    public Integer missionID;
    public String name;
    public String beginDate;
    public String endDate;
    public Integer author;
    public Integer executor;

    public void setData(String[] data) {
        missionID = Integer.parseInt(data[0]);
        name = data[1];
        beginDate = data[2];
        endDate = data[3];
        author = Integer.parseInt(data[4]);
        executor = Integer.parseInt(data[5]);
    }

    @Override
    public int compareTo(MissionWritable o) {
        return this.missionID.compareTo(o.missionID);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(missionID);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(beginDate);
        dataOutput.writeUTF(endDate);
        dataOutput.writeInt(author);
        dataOutput.writeInt(executor);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        missionID = dataInput.readInt();
        name = dataInput.readUTF();
        beginDate = dataInput.readUTF();
        endDate = dataInput.readUTF();
        author = dataInput.readInt();
        executor = dataInput.readInt();
    }

    @Override
    public String toString() {
        return missionID + "\t" + name + "\t" + beginDate + "\t" + endDate
                + "\t" + author + "\t" + executor;
    }
}
