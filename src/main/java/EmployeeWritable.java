import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EmployeeWritable implements WritableComparable<EmployeeWritable> {

    public Integer serviceNumber;
    public String firstName;
    public String lastName;
    public String middleName;
    public String phone;
    public String email;
    public String fax;
    public Integer positionId;

    public void setData(String[] data) {
        serviceNumber = Integer.parseInt(data[0]);
        firstName = data[1];
        lastName = data[2];
        middleName = data[3];
        phone = data[4];
        email = data[5];
        fax = data[6];
        positionId = Integer.parseInt(data[7]);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        serviceNumber = in.readInt();
        firstName = in.readUTF();
        lastName = in.readUTF();
        middleName = in.readUTF();
        phone = in.readUTF();
        email = in.readUTF();
        fax = in.readUTF();
        positionId = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(serviceNumber);
        out.writeUTF(firstName);
        out.writeUTF(lastName);
        out.writeUTF(middleName);
        out.writeUTF(phone);
        out.writeUTF(email);
        out.writeUTF(fax);
        out.writeInt(positionId);
    }

    @Override
    public int compareTo(EmployeeWritable o) {
        return this.serviceNumber.compareTo(o.serviceNumber);
    }

    @Override
    public String toString() {
        return serviceNumber + "\t" + firstName + "\t" + lastName + "\t" + middleName
                + "\t" + phone + "\t" + email + "\t" + fax + "\t" + positionId;
    }
}
