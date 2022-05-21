import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class Relation {

    // SELECT * FROM EMPLOYEE WHERE FirstName = 'Clarissa';
    public static class Select1Mapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            StringBuilder line = new StringBuilder();
            int index = 0;
            Text id = null;
            while (itr.hasMoreTokens()) {
                if (index == 0) {
                    id = new Text(itr.nextToken());
                } else {
                    String word = itr.nextToken();
                    if (index == 1 && !word.equals("Clarissa")) {
                        return;
                    }
                    line.append(word).append("\t");
                }
                index++;
            }
            context.write(id, new Text(line.toString()));
        }
    }

    public static class Select1Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            for (Text text : value) {
                context.write(key, text);
            }
        }
    }

    // SELECT * FROM Mission WHERE Executor > 4;
    public static class Select2Mapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            StringBuilder line = new StringBuilder();
            int index = 0;
            Text id = null;
            while (itr.hasMoreTokens()) {
                if (index == 0) {
                    id = new Text(itr.nextToken());
                } else {
                    String word = itr.nextToken();
                    if (index == 5 && !(Integer.parseInt(word) > 4)) {
                        return;
                    }
                    line.append(word).append("\t");
                }
                index++;
            }
            context.write(id, new Text(line.toString()));
        }
    }

    public static class Select2Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            for (Text text : value) {
                context.write(key, text);
            }
        }
    }

    // SELECT * FROM Mission M FULL OUTER JOIN EMPLOYEE E ON E.ServiceNumber = M.Author;
    public static class Select3Mapper extends Mapper<Object, Text, Text, ObjectWritable> {

        private final Text outputKey = new Text();
        private final ObjectWritable outputValue = new ObjectWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(" ");
            if (data.length == 6) {
                MissionWritable mission = new MissionWritable();
                mission.setData(data);
                outputKey.set(String.valueOf(mission.author));
                outputValue.set(mission);
            } else {
                EmployeeWritable employee = new EmployeeWritable();
                employee.setData(data);
                outputKey.set(String.valueOf(employee.serviceNumber));
                outputValue.set(employee);
            }
            context.write(outputKey, outputValue);
        }
    }

    public static class Select3Reducer extends Reducer<Text, ObjectWritable, Text, Text> {

        private final MissionWritable missionNull = new MissionWritable();
        private final EmployeeWritable employeeNull = new EmployeeWritable();

        private final Text missionOutput = new Text();
        private final Text employeeOutput = new Text();

        private final List<String> missionList = new ArrayList<>();
        private final List<String> employeeList = new ArrayList<>();

        @Override
        public void reduce(Text keyId, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {

            missionList.clear();
            employeeList.clear();

            setDataToLists(values, missionList, employeeList);

            // Joining list data (FULL OUTER JOIN)
            if (!missionList.isEmpty()) {
                for (String missionStr : missionList) {
                    if (!employeeList.isEmpty()) {
                        // Case 1 : Both parts have data
                        for (String employeeStr : employeeList) {
                            missionOutput.set(missionStr);
                            employeeOutput.set(employeeStr);
                            context.write(missionOutput, employeeOutput);
                        }
                    } else {
                        // Case 2 : There is data on the task, but not on the employee(author)
                        missionOutput.set(missionStr);
                        employeeOutput.set(employeeNull.toString());
                        context.write(missionOutput, employeeOutput);
                    }
                }
            } else {
                // Case 3 : There is no data on the task, but on the employee(author) there is
                for (String employeeStr : employeeList) {
                    missionOutput.set(missionNull.toString());
                    employeeOutput.set(employeeStr);
                    context.write(missionOutput, employeeOutput);
                }
            }
        }
    }

    private static void setDataToLists(Iterable<ObjectWritable> values,
                                       List<String> missionList,
                                       List<String> employeeList) throws IOException {
        for (ObjectWritable value : values) {
            Object object = value.get();
            if (object instanceof MissionWritable) {
                MissionWritable mission = (MissionWritable) object;
                String missionOutput = mission.toString();
                missionList.add(missionOutput);
            } else if (object instanceof EmployeeWritable) {
                EmployeeWritable employee = (EmployeeWritable) object;
                String employeeOutput = employee.toString();
                employeeList.add(employeeOutput);
            } else {
                throw new IOException();
            }
        }
    }

    // SELECT * FROM Position P JOIN EMPLOYEE E ON E.PositionID = P.PositionID;
    public static class Select4Mapper extends Mapper<Object, Text, Text, ObjectWritable> {

        private final Text outputKey = new Text();
        private final ObjectWritable outputValue = new ObjectWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(" ");
            if (data.length == 2) {
                PositionWritable position = new PositionWritable();
                position.positionId = Integer.parseInt(data[0]);
                position.title = data[1];
                outputKey.set(String.valueOf(position.positionId));
                outputValue.set(position);
            } else {
                EmployeeWritable employee = new EmployeeWritable();
                employee.setData(data);
                outputKey.set(String.valueOf(employee.positionId));
                outputValue.set(employee);
            }
            context.write(outputKey, outputValue);
        }
    }

    public static class Select4Reducer extends Reducer<Text, ObjectWritable, Text, Text> {

        private final Text positionOutput = new Text();
        private final Text employeeOutput = new Text();

        private final List<String> positionList = new ArrayList<>();
        private final List<String> employeeList = new ArrayList<>();

        @Override
        public void reduce(Text keyId, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {

            positionList.clear();
            employeeList.clear();

            for (ObjectWritable value : values) {
                Object object = value.get();
                if (object instanceof PositionWritable) {
                    PositionWritable position = (PositionWritable) object;
                    String missionOutput = position.toString();
                    positionList.add(missionOutput);
                } else if (object instanceof EmployeeWritable) {
                    EmployeeWritable employee = (EmployeeWritable) object;
                    String employeeOutput = employee.toString();
                    employeeList.add(employeeOutput);
                } else {
                    throw new IOException();
                }
            }

            innerJoin(context, positionList, employeeList, positionOutput, employeeOutput);
        }
    }

    private static void innerJoin(Reducer<Text, ObjectWritable, Text, Text>.Context context,
                                  List<String> firstList,
                                  List<String> secondList,
                                  Text firstOutput,
                                  Text secondOutput) throws IOException, InterruptedException {
        // Joining list data (INNER JOIN)
        if (!firstList.isEmpty()) {
            for (String positionStr : firstList) {
                if (!secondList.isEmpty()) {
                    // Case 1 : Both parts have data
                    for (String employeeStr : secondList) {
                        firstOutput.set(positionStr);
                        secondOutput.set(employeeStr);
                        context.write(firstOutput, secondOutput);
                    }
                }
            }
        }
    }


    // SELECT * FROM Mission M JOIN EMPLOYEE E ON E.ServiceNumber = M.Executor WHERE E.FirstName LIKE '%a%';
    public static class Select5Mapper extends Mapper<Object, Text, Text, ObjectWritable> {

        private final Text outputKey = new Text();
        private final ObjectWritable outputValue = new ObjectWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(" ");
            if (data.length == 6) {
                MissionWritable mission = new MissionWritable();
                mission.setData(data);
                outputKey.set(String.valueOf(mission.executor));
                outputValue.set(mission);
                context.write(outputKey, outputValue);
            } else if (data[1].contains("а")){
                EmployeeWritable employee = new EmployeeWritable();
                employee.setData(data);
                outputKey.set(String.valueOf(employee.serviceNumber));
                outputValue.set(employee);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class Select5Reducer extends Reducer<Text, ObjectWritable, Text, Text> {

        private final Text missionOutput = new Text();
        private final Text employeeOutput = new Text();

        private final List<String> missionList = new ArrayList<>();
        private final List<String> employeeList = new ArrayList<>();

        @Override
        public void reduce(Text keyId, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {

            missionList.clear();
            employeeList.clear();

            setDataToLists(values, missionList, employeeList);

            innerJoin(context, missionList, employeeList, missionOutput, employeeOutput);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        String job1Name = "relation";
        String inputPath = args[0];
        String outputPath = args[1] + "/" + job1Name;

        Job job = Job.getInstance(conf, job1Name);
        job.setJarByClass(Relation.class);

        switch (args[2]) {
            case "select1":
                setConfForSelect1(job);
                break;
            case "select2":
                setConfForSelect2(job);
                break;
            case "select3":
            setConfForSelect3(job);
                break;
            case "select4":
            setConfForSelect4(job);
                break;
            case "select5":
            setConfForSelect5(job);
                break;
            default:
                throw new RuntimeException();
        }
        if (args[0].contains(",")) {
            FileInputFormat.addInputPaths(job, args[0]);
        } else {
            FileInputFormat.setInputPaths(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (!job.waitForCompletion(true))
            System.exit(1);
    }

    private static void setConfForSelect1(Job job) {
        job.setMapperClass(Select1Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Select1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    }

    private static void setConfForSelect2(Job job) {
        job.setMapperClass(Select2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Select2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    }

    private static void setConfForSelect3(Job job) {
        job.setMapperClass(Select3Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ObjectWritable.class);

        job.setReducerClass(Select3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    }

    private static void setConfForSelect4(Job job) {
        job.setMapperClass(Select4Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ObjectWritable.class);

        job.setReducerClass(Select4Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    }

    private static void setConfForSelect5(Job job) {
        job.setMapperClass(Select5Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ObjectWritable.class);

        job.setReducerClass(Select5Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    }
}
