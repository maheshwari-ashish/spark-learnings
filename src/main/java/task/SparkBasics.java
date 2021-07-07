package task;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import task.dto.Employee;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class SparkBasics implements Serializable {

	private final Pattern SPACE;
	private final String EMPLOYEE_PATH;
	private final String DEPARTMENT_PATH;
	private final String WORDS_PATH;
	private static final Logger log = Logger.getLogger(SparkBasics.class);

	public SparkBasics() {
		SPACE = Pattern.compile(" ");
		EMPLOYEE_PATH = "/Users/ashishmaheshwari/spark data/emp.csv";
		DEPARTMENT_PATH = "/Users/ashishmaheshwari/spark data/dept.csv";
		WORDS_PATH = "/Users/ashishmaheshwari/spark data/wordCount.txt";
	}

	public static void main(String[] args) {

		SparkSession spark = SparkSession
				.builder()
				.appName("Spark Demo")
				.config(getSparkConfig())
				.getOrCreate();

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkBasics sparkBasics = new SparkBasics();

		sparkBasics.simpleWordCount(spark);

		sparkBasics.employeeStatsWithDatasetAndSql(spark);

		sparkBasics.employeeStatsWithRDD(spark);

		sparkBasics.datasetVsDataFrameVsJavaRDD(spark);

		sparkBasics.datasetWithCaching(spark);

		sparkBasics.multipleSessionsWithJoin(spark);

		spark.stop();
	}

	/*
	1.	Read employee data using existing session and store in DF
	2.	Create new session with different configuration and read department data using new session
	3.	Join both DFs and show result
	 */
	private void multipleSessionsWithJoin(SparkSession spark) {

		Dataset<Row> empDataset = readEmployeeData(spark);

		log.info("Spark Session old :: " + spark);

		log.info("Spark Context old :: " + spark.sparkContext());

		SparkSession.clearActiveSession();
		SparkSession.clearDefaultSession();

		SparkSession newSession = SparkSession
				.builder()
				.appName("Spark New Session")
				.config("spark.eventLog.enabled", "true")
				.config("spark.driver.cores", "2")
				.getOrCreate();

		SparkSession.setDefaultSession(newSession);

		Dataset<Row> deptDataset = readDepartmentData(newSession);

		deptDataset
				.join(
						empDataset,
						deptDataset.col("deptno").equalTo(empDataset.col("deptno")),
						"inner"
				)
				.show();

		log.info("Spark Session new :: " + newSession);

		log.info("Spark Context new :: " + newSession.sparkContext());

	}

	private void datasetWithCaching(SparkSession spark) {
		log.info("*****datasetWithCaching*****");

		Dataset<Row> empData = readEmployeeData(spark);

		empData.createOrReplaceTempView("empView");

		Dataset<Employee> ds = spark.sql("SELECT empno,ename,job,mgr,hiredate,sal,comm,deptno FROM empView")
				.as(Encoders.bean(Employee.class));

		ds.cache();

		ds.groupBy("mgr").count().show();

		ds.groupBy("deptno").count().show();

		ds.select("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")
				.filter("empno = 7900 ")
				.show();

	}

	private void employeeStatsWithRDD(SparkSession spark) {

		log.info("*****employeeStatsWithRDD*****");

		JavaRDD<Row> empData = readEmployeeData(spark).javaRDD();

		JavaRDD<Row> deptData = readDepartmentData(spark).javaRDD();

		JavaPairRDD<Integer, Row> pairEmp = empData.mapToPair(x -> new Tuple2<>(x.getInt(7), x));

		JavaPairRDD<Integer, Row> pairDept = deptData.mapToPair(x -> new Tuple2<>(x.getInt(0), x));

		JavaPairRDD<Integer, Tuple2<Row, Row>> joinPair = pairEmp.join(pairDept).filter(x -> x._2._1.getInt(0) == 7654);

		joinPair.foreach(x -> System.out.println("dept no :: " + x._1() + " data :: " + x._2()));
	}

	private Dataset<Row> readDepartmentData(SparkSession spark) {
		return spark
				.read()
				.format("csv")
				.option("inferSchema", "true")
				.option("header", "true")
				.load(DEPARTMENT_PATH);
	}

	private StructType getEmpSchema() {
		return DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("empno", DataTypes.IntegerType, true),
				DataTypes.createStructField("ename", DataTypes.StringType, true),
				DataTypes.createStructField("job", DataTypes.StringType, true),
				DataTypes.createStructField("mgr", DataTypes.IntegerType, true),
				DataTypes.createStructField("hiredate", DataTypes.StringType, true),
				DataTypes.createStructField("sal", DataTypes.IntegerType, true),
				DataTypes.createStructField("comm", DataTypes.DoubleType, true),
				DataTypes.createStructField("deptno", DataTypes.IntegerType, true)
		});
	}

	private static SparkConf getSparkConfig() {
		SparkConf config = new SparkConf();
		config.set("spark.eventLog.enabled", "true");
		config.set("spark.driver.cores", "4");
		config.set("spark.driver.memory", "1G");
		config.set("spark.executor.memory", "1G");
		return config;
	}

	private void datasetVsDataFrameVsJavaRDD(SparkSession spark) {

		log.info("*****datasetVsDataFrameVsJavaRDD*****");

		Dataset<Row> empData = readEmployeeData(spark);

		empData.createOrReplaceTempView("empView");

		Dataset<Employee> employeeDataset = spark
				.sql("SELECT empno,ename,job,mgr,hiredate,sal,comm,deptno FROM empView")
				.as(Encoders.bean(Employee.class));

//		To drop some columns
//		emp.filter("empno = 7698").drop("job", "comm").show();

		log.info("dataset filter");
		employeeDataset
				.filter((FilterFunction<Employee>) employee -> employee.getEmpno().equals(7698))
				.show();

		log.info("dataframe filter");
		employeeDataset
				.toDF()
				.filter((FilterFunction<Row>) employee -> employee.getInt(0) == 7698)
				.show();

		log.info("java RDD filter");
		employeeDataset
				.toJavaRDD()
				.filter(employee -> employee.getEmpno() == 7698)
				.foreach(employee -> System.out.println(employee));

		/*
			https://github.com/spirom/learning-spark-with-java/blob/master/src/main/java/rdd/Basic.java#L50
			NOTE: in the above it may be tempting to replace the above
			lambda expression with a method reference -- System.out::println --
			but alas this results in
				java.io.NotSerializableException: java.io.PrintStream
			https://stackoverflow.com/a/63322374/10302921
		*/

//		To convert JavaRDD to Dataset
// 		spark.createDataset(emp.rdd(), Encoders.bean(Employee.class));

	}

	private Dataset<Row> readEmployeeData(SparkSession spark) {
		return spark
				.read()
				.format("csv")
				.schema(getEmpSchema())
				.option("header", "true")
				.load(EMPLOYEE_PATH);
	}

	/*
	1.	Read employee and department data into respective DFs
	2.	Create views out of DFs
	3.	Run SQL join query and show result
	 */
	private void employeeStatsWithDatasetAndSql(SparkSession spark) {

		log.info("*****employeeStatsWithDatasetAndSql*****");

		Dataset<Row> empData = readEmployeeData(spark);

		Dataset<Row> deptData = readDepartmentData(spark);

		log.info("Reading complete. Showing data");

		empData.show();

		deptData.show();

		log.info("Creating views for employee and department data");

		empData.createOrReplaceTempView("empView");
		deptData.createOrReplaceTempView("deptView");

		log.info("Showing details for empno 7654");

		spark.sql(
				"SELECT empno, ename, job, dname "
						+	"FROM empView "
						+	"INNER JOIN deptView on empView.deptno = deptView.deptno "
						+	"WHERE empno = 7654"
				)
				.show();

	}

	/*
	1.	Read file contents into RDD
	2.	Add each occurrence of word in a pair with count as 1
	3.	Apply reduce function to group by word and sum up its count
	 */
	private void simpleWordCount(SparkSession spark) {

		log.info("*****simpleWordCount*****");

		JavaRDD<String> lines = spark
				.read()
				.textFile(WORDS_PATH)
				.javaRDD();

		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<String,Integer> tuple : output) {
			System.out.println(tuple._1() + " :: " + tuple._2());
		}
	}
}
