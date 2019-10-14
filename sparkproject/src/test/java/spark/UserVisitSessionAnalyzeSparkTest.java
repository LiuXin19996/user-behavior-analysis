package spark;

import com.alibaba.fastjson.JSONObject;
import com.lx.sparkproject.constant.Constants;
import com.lx.sparkproject.dao.ITaskDAO;
import com.lx.sparkproject.dao.impl.DAOFactory;
import com.lx.sparkproject.domian.Task;
import com.lx.sparkproject.spark.session.SessionAggrStatAccumulator;
import com.lx.sparkproject.util.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.*;

import static com.lx.sparkproject.constant.Constants.*;
import static com.lx.sparkproject.util.SparkUtils.getActionRDDByDateRange;

public class UserVisitSessionAnalyzeSparkTest {

    public static void main(String[] args) {

        args = new String[]{"2"};

        SparkConf conf = new SparkConf().setAppName("UserVisitSessionAnalyzeSparkTest").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        // 生成模拟测试数据
        SparkUtils.mockData(sc, sqlContext);

        // 创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        // 首先得查询出来指定的任务，并获取任务的查询参数
        long taskid = ParamUtils.getTaskIdFromArgs(args, SPARK_LOCAL_TASKID_SESSION);
        Task task = taskDAO.findById(taskid);

        if(task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
            return;
        }

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        System.out.println("taskParam: "+ taskParam);

        // 如果要进行session粒度的数据聚合
        // 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        System.out.println("actionRDD:"+ actionRDD.take(15));
        //<row> => <sessionid,row>
        JavaPairRDD<String, Row> sessionId2actionRDD = getSessionId2ActionRDD(actionRDD);

       sessionId2actionRDD.foreach(new VoidFunction<Tuple2<String, Row>>() {
           @Override
           public void call(Tuple2<String, Row> t2) throws Exception {
               System.out.println("sessionId2actionRDD   sessionId: "+t2._1+"  row:"+t2._2);
           }
       });

        //<sessionid,"sessionid\searchKeywords\clickCategoryIds\visitLength\stepLength\startTime\age\professional\city\sex">
        JavaPairRDD<String, String> sessionId2AggrInfoRDD =  aggrBySession(sqlContext, actionRDD);

         sessionId2AggrInfoRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
             @Override
             public void call(Tuple2<String, String> t2) throws Exception {
                 System.out.println("sessionId2AggrInfoRDD  sessionId: "+t2._1+"拼接的字段："+ t2._2);
             }
         });

        // 累加器，统计访问步长，访问时长
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
                "", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggreStat(
                sessionId2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);

        filteredSessionid2AggrInfoRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> t2) throws Exception {

                System.out.println(" filteredSessionid2AggrInfoRDD： "+t2._1+"   "+ t2._2);
            }
        });

        // 生成公共的RDD：通过筛选条件的session的访问明细数据
        JavaPairRDD<String, Row> sessionid2detailRDD = getSessionId2detailRDD(
                filteredSessionid2AggrInfoRDD, sessionId2actionRDD);

//        //过滤后的数据，格式为<sessionid,Row>
//        sessionid2detailRDD.foreach(new VoidFunction<Tuple2<String, Row>>() {
//            @Override
//            public void call(Tuple2<String, Row> t2) throws Exception {
//
//                System.out.println("sessionid2detailRDD: "+t2._1+"  "+t2._2);
//            }
//        });

        randomExtrSession(task.getTaskId(),
                filteredSessionid2AggrInfoRDD, sessionId2actionRDD);













        sc.close();




    }

    private static void randomExtrSession(long taskId, JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
                                          JavaPairRDD<String, Row> sessionId2actionRDD) {


        JavaPairRDD<String, String> Rdd1 = filteredSessionid2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

            @Override
            public Tuple2<String, String> call(Tuple2<String, String> t2) throws Exception {

                String aggrInfo = t2._2;

                String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", FIELD_START_TIME);

                String dateHour = DateUtils.getDateHour(startTime);

                return new Tuple2<String, String>(dateHour, aggrInfo);

            }
        });


        //得到了每天每小时的session的数量
        Map<String, Object> countMap = Rdd1.countByKey();


        /**
         * 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
         */

        // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
        Map<String, Map<String, Long>> dateHourCountMap =
                new HashMap<String, Map<String, Long>>();

        for(Map.Entry<String, Object> entries:countMap.entrySet()){

            String dateHour = entries.getKey();

            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            long count = Long.parseLong(String.valueOf(entries.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);

            if(hourCountMap==null){

                Map<String, Long> hourCoutMap = new HashMap<String, Long>();

                dateHourCountMap.put(date,hourCountMap);

            }

            hourCountMap.put(hour,count);

        }




    }

    private static JavaPairRDD<String, Row> getSessionId2detailRDD(JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
                                                                   JavaPairRDD<String, Row> sessionId2actionRDD) {

        JavaPairRDD<String, Tuple2<String, Row>> Rdd1 = filteredSessionid2AggrInfoRDD.join(sessionId2actionRDD);

        JavaPairRDD<String, Row> Rdd2 = Rdd1.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {

            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> t2) throws Exception {

                return new Tuple2<String, Row>(t2._1, t2._2._2);
            }
        });

        return  Rdd2;
    }

    private static JavaPairRDD<String, String> filterSessionAndAggreStat(JavaPairRDD<String, String> sessionId2AggrInfoRDD,
                                                                         JSONObject taskParam, Accumulator<String> sessionAggrStatAccumulator) {
        // 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
        // 此外，这里其实大家不要觉得是多此一举
        // 其实我们是给后面的性能优化埋下了一个伏笔
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");

        final String parameter = _parameter;

        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD = sessionId2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String>  t2) throws Exception {

                String aggrInfo = t2._2;

                if(!ValidUtils.between(aggrInfo, FIELD_AGE,parameter, PARAM_START_AGE,PARAM_END_AGE)){
                    return false;
                }

                if(!ValidUtils.in(aggrInfo,FIELD_PROFESSIONAL,parameter,PARAM_PROFESSIONALS)){
                    return  false;
                }

                if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                        parameter, Constants.PARAM_CITIES)) {
                    return false;
                }

                if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                        parameter, Constants.PARAM_SEX)) {
                    return false;
                }

                if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                        parameter, Constants.PARAM_KEYWORDS)) {
                    return false;
                }

                if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                        parameter, Constants.PARAM_CATEGORY_IDS)) {
                    return false;
                }

                // 主要走到这一步还没有别过滤，就需要增加session的计数了
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);


                long visitLength = Long.parseLong(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", FIELD_VISIT_LENGTH));

                long stepLength = Long.parseLong(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", FIELD_STEP_LENGTH));

                calVisitLength(visitLength);
                calStepLength(stepLength);


                return true;
            }

            private void calStepLength(long stepLength) {

                if(stepLength >= 1 && stepLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if(stepLength >= 4 && stepLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if(stepLength >= 7 && stepLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if(stepLength >= 10 && stepLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if(stepLength > 30 && stepLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if(stepLength > 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }


            private void calVisitLength(long visitLength) {
                if(visitLength >=1 && visitLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if(visitLength >=4 && visitLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if(visitLength >=7 && visitLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if(visitLength >=10 && visitLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if(visitLength > 30 && visitLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if(visitLength > 60 && visitLength <= 180) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if(visitLength > 180 && visitLength <= 600) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if(visitLength > 600 && visitLength <= 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if(visitLength > 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }
            }

        });


        return  filteredSessionId2AggrInfoRDD;
    }


    /**\
     * 		StructType schema = DataTypes.createStructType(Arrays.asList(
     * 			0	DataTypes.createStructField("date", DataTypes.StringType, true),
     * 			1	DataTypes.createStructField("user_id", DataTypes.LongType, true),
     * 			2	DataTypes.createStructField("session_id", DataTypes.StringType, true),
     * 			3	DataTypes.createStructField("page_id", DataTypes.LongType, true),
     * 			4	DataTypes.createStructField("action_time", DataTypes.StringType, true),
     * 			5	DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
     * 			6	DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
     * 			7	DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
     * 			8	DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
     * 			9	DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
     * 			10	DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
     * 			11	DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
     * 			12	DataTypes.createStructField("city_id", DataTypes.LongType, true)));
     *
     * 	                  String value = FIELD_SESSION_ID + "=" + row.getString(2)+
     *                                "|"+FIELD_SEARCH_KEYWORDS+"="+row.getString(5)+
     *                                "|"+FIELD_CLICK_CATEGORY_IDS+"="+row.getString(6)+
     *                                "|"+FIELD_VISIT_LENGTH+"="+row.getString(6)+
     *                                "|"+FIELD_STEP_LENGTH+"="+row.getString(6)+
     *                                "|"+FIELD_STEP_LENGTH+"="+row.getString(6)+"|";
     *
     * @param sqlContext
     * @param actionRDD
     * @return
     */

    private static JavaPairRDD<String, String> aggrBySession(SQLContext sqlContext, JavaRDD<Row> actionRDD) {


        JavaPairRDD<String, Row> Rdd1 = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {

                String sessionid = row.getString(2);

                return new Tuple2<String, Row>(sessionid, row);
            }
        });

        JavaPairRDD<String, Iterable<Row>> Rdd2 = Rdd1.groupByKey();

        JavaPairRDD<Long, String> Rdd3 = Rdd2.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>,Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> t2) throws Exception {

                String sessionid = t2._1;
                Iterator<Row> it = t2._2.iterator();

                StringBuffer searchKeywordsBuffer = new StringBuffer("");
                StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                Long userid = null;

                // session的起始和结束时间
                Date startTime = null;
                Date endTime = null;
                // session的访问步长
                int stepLength = 0;

                while (it.hasNext()){

                    Row _row = it.next();

                    if(userid==null){
                        userid=_row.getLong(1);
                    }

                    String searchWord = _row.getString(5);

                    if(StringUtils.isNotEmpty(searchWord)){

                        if(!searchKeywordsBuffer.toString().contains(searchWord)){

                            searchKeywordsBuffer.append(searchWord+",");
                        }

                    }

                    Long clickCategoryId = _row.getLong(6);

;                    if(clickCategoryId!=null){

                       if(!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
                           clickCategoryIdsBuffer.append(String.valueOf(clickCategoryId));
                       }

                    }

                    Date actionTime = DateUtils.parseTime(_row.getString(4));

                    if(startTime==null){

                        startTime=actionTime;

                    }

                    if(endTime==null){

                        endTime=actionTime;
                    }

                    if(actionTime.before(startTime)){

                        startTime=actionTime;

                    }

                    if(actionTime.after(endTime)){

                        endTime=actionTime;
                    }

                    //计算访问步长
                    stepLength++;

                }

                String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                // 计算session访问时长（秒）
                long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                // key=value|key=value
                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                        + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                        + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);

                return new Tuple2<Long, String>(userid,partAggrInfo);
            }
        });

        /**
         *
         * 		StructType schema2 = DataTypes.createStructType(Arrays.asList(
         * 			0	DataTypes.createStructField("user_id", DataTypes.LongType, true),
         * 			1	DataTypes.createStructField("username", DataTypes.StringType, true),
         * 			2	DataTypes.createStructField("name", DataTypes.StringType, true),
         * 			3	DataTypes.createStructField("age", DataTypes.IntegerType, true),
         * 			4	DataTypes.createStructField("professional", DataTypes.StringType, true),
         * 			5	DataTypes.createStructField("city", DataTypes.StringType, true),
         * 			6	DataTypes.createStructField("sex", DataTypes.StringType, true)));
         *
         */

        String sql = "select * from user_info";

        JavaPairRDD<Long, Row> Rdd4 = sqlContext.sql(sql).javaRDD().mapToPair(new PairFunction<Row, Long, Row>() {

            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {

                long userid = row.getLong(0);


                return new Tuple2<Long, Row>(userid, row);
            }
        });

        JavaPairRDD<Long, Tuple2<String, Row>> Rdd5 = Rdd3.join(Rdd4);


        JavaPairRDD<String, String> Rdd6 = Rdd5.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> t2) throws Exception {

                String partAggrInfo = t2._2._1;

                String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", FIELD_SESSION_ID);

                Row _row = t2._2._2;

                Integer age = _row.getInt(3);
                String professional = _row.getString(4);
                String city = _row.getString(5);
                String sex = _row.getString(6);

                String fullAggrInfo = partAggrInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;


                return new Tuple2<String, String>(sessionId,fullAggrInfo);
            }
        });


        return  Rdd6;
    }







    private static JavaPairRDD<String, Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD) {


        JavaPairRDD<String, Row> sessionId2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {

                String sessionId = row.getString(2);

                return new Tuple2<String, Row>(sessionId,row);
            }
        });

        return  sessionId2ActionRDD;
    }
}
