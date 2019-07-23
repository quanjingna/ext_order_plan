from pyspark.sql import SparkSession
from pyspark import StorageLevel
from easy_forecast.config.naming_specification import DATETIME_STAMP, Y, Y_HAT, Y_HAT_LOWER, Y_HAT_UPPER, OBJ_NO
from pyspark.sql import Row
from pyspark.sql.types import *
import pandas as pd
import datetime
import time
#from pyspark.sql import SQLContext
# from easy_forecast.batch.simple_data_process_strategies import SimpleBatchDataProcess
# from easy_forecast.batch.simple_forecast_strategies import SimpleStrategies
from easy_forecast.validation.forecast_validation import BackValidation
from easy_forecast.batch.simple_data_process_strategies import SimpleBatchDataProcess
from easy_forecast.strategies.ts_day_base_multiseasonal.base_strategies import SimpleStrategies

spark = SparkSession.builder.appName("seller_forecast_online").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
#sqlContext = SQLContext(sc)

#spark.sqlContext.setConf("hive.merge.mapfiles", "true")
#spark.sqlContext.setConf("mapred.max.split.size", "256000000")
#spark.sqlContext.setConf("mapred.min.split.size.per.node", "192000000")
#spark.sqlContext.setConf("mapred.min.split.size.per.rack", "192000000")
#spark.sqlContext.setConf("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")

date_rate=(datetime.date.today() + datetime.timedelta(days=-90)).strftime("%Y-%m-%d")
dt=(datetime.date.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
#brdcst_cutoff_date = sc.broadcast(str(pd.to_datetime(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())[0:10]) - datetime.timedelta(1))[0:10])


def get_data(spark):
    """
    获取数据
    :param spark:
    :return:
    """
    sql = "select	aa.op_date as ds,	aa.seller_no,	aa.seller_name,	aa.dept_no,	aa.dept_name,	aa.region_name,	count(distinct aa.biz_no) as y,	aa.warehouse_no,	aa.warehouse_name from	(		SELECT			substr(a.op_date, 1, 10) as op_date,			a.dept_no,			b.dept_name,			a.biz_no,			b.seller_no,			b.seller_name,			a.region_name,			kk.warehouse_no,			kk.warehouse_name		FROM			(				select					op_date,					dept_no,					biz_no,					region_name,					distribute_no,					warehouse_no				from					app.app_store_prophet_ob_sale_eclp				where					dt >= '2018-05-01'              and distribute_no<>'NULL' and warehouse_no<>'NULL' and dept_no<>'NULL'and region_name <> 'NULL'			)			a		left join			(				select					*				from					fdm.fdm_eclp_master_warehouse_chain				where					dp = 'ACTIVE'					and yn = 1					and start_date <= '" + dt + "'				and end_date >= '" + dt + "'			)			kk		on			a.distribute_no = kk.distribution_no			and a.warehouse_no = kk.erp_warehouse_no		inner join			(				select					a1.dept_no,					a1.dept_name,					a2.seller_name,					a2.seller_no				from					(						select							dept_no,							dept_name,							seller_id						from							fdm.fdm_eclp_master_dept_chain						where							dp = 'ACTIVE'							and start_date <= '" + dt + "'				and end_date >= '" + dt + "'					)					a1				inner join					(						select							id,							seller_no,							seller_name						from							fdm.fdm_eclp_master_seller_chain						where							dp = 'ACTIVE'							and start_date <= '" + dt + "'				and end_date >= '" + dt + "'					)					a2				on					a1.seller_id = a2.id			)			b on a.dept_no = b.dept_no	)	aa group by	aa.op_date,	aa.region_name,	aa.seller_no,	aa.seller_name,	aa.dept_no,	aa.dept_name,	aa.warehouse_no,	aa.warehouse_name"
    wh_data = spark.sql(sql)
    wh_data.createOrReplaceTempView("wh_data")
    sql = " select	wh.region_name,	wh.seller_no,	wh.dept_no,	wh.warehouse_no,	case		when a.wh_y is null		then 0		else a.wh_y	end as wh_y,	b.dp_y,	case		when (a.wh_y / b.dp_y) is null then  0		else (a.wh_y / b.dp_y)	end as rate from	(		select			region_name,			seller_no,			dept_no,			warehouse_no		from			wh_data		group by			region_name,			seller_no,			dept_no,			warehouse_no	)	wh left join	(		select			region_name,			seller_no,			dept_no,			warehouse_no,			sum(y) as wh_y		from			wh_data		where			ds >= '" + date_rate + "'		group by			region_name,			seller_no,			dept_no,			warehouse_no	)	a on	a.region_name = wh.region_name	and a.seller_no = wh.seller_no	and a.dept_no = wh.dept_no and a.warehouse_no =wh.warehouse_no left join	(		select			region_name,			seller_no,			dept_no,			sum(y) as dp_y		from			wh_data		where			ds >= '" + date_rate + "'		group by			region_name,			seller_no,			dept_no	)	b on	a.region_name = b.region_name	and a.seller_no = b.seller_no	and a.dept_no = b.dept_no"
    rate_data = spark.sql(sql)
    rate_data.createOrReplaceTempView("rate_data")
    raw_data = spark.sql(
        """ SELECT ds, sum(y) as y , concat(a.region_name, '_', a.seller_no, '_',  dept_no) AS obj_str FROM wh_data a group by ds, concat(a.region_name, '_', a.seller_no,  '_', dept_no)""")
    raw_data.createOrReplaceTempView("raw_data")
    # 分组个数
    sql = "select row_number() over(order by obj_str) as obj_no , obj_str from raw_data group by obj_str"
    obj_no_list = spark.sql(sql)
    obj_no_list.createOrReplaceTempView("obj_no_list")
    # spark.sql('select * from obj_no_list where obj_no is null').show()
    # print(obj_no_list.select("obj_str").distinct().count())
    # 将分组序号添加到元数据
    sql = "select a.*, b.obj_no from raw_data a left join obj_no_list b on a.obj_str = b.obj_str "
    lines = spark.sql(sql)
    lines = lines.dropna()
    return lines

def back_testing(index, lines):
    print("index:{}".format(index))
    # 转化为pandas.dataframe
    data_list = []
    for item in lines:
        row = item[1]
        data_list.append(row.asDict())
    lines = pd.DataFrame(data_list)
    if lines.shape[0]==0:
        return iter([])
    cut_off_date = str(pd.to_datetime(brdcst_cutoff_date.value))[0:10]
    back_date = pd.to_datetime(brdcst_cutoff_date.value) - datetime.timedelta(days=brdcst_forecasting_periods.value)
    min_date = pd.to_datetime(lines.ds.min())
    if min_date > back_date:
        return iter([])
    # raw_data_process = SimpleBatchDataProcess(lines, cutoff_date=cut_off_date, freq='d')
    # lines = raw_data_process.compute(use_pool=None)
    # 判断回测之前的数据是否为空
    # pd.to_datetime()
    back_valid = BackValidation(df=lines,data_process_class=SimpleBatchDataProcess,
                                forecast_strategies_class=SimpleStrategies, cutoff_date=cut_off_date,
                                forecasting_freq="d", test_period_n=brdcst_forecasting_periods.value)
    rs = back_valid.back_testing(loop=False,use_pool=False).tail(90)
    rs=rs[[OBJ_NO,Y_HAT,Y,DATETIME_STAMP]]
    #batch = SimpleStrategies(lines, freq='d', forecast_periods=90)
    #rs = batch.compute(use_pool=None).tail(90)
    # 转化为Row list
    def get_row(line):
        d = line.to_dict()
        for name in [Y_HAT,Y]:
            d[name] = float(d[name])
        d[DATETIME_STAMP] = d[DATETIME_STAMP].strftime("%Y-%m-%d")
        return Row(**d)
    rs_convert = rs.apply(lambda line: get_row(line), axis=1)
    return iter(list(rs_convert))

def self_partition(obj):
    return int(obj)

def deal_result(forecast_rs):
    forecast_rs.createOrReplaceTempView("forecast_rs")
    sql = "select a.*, b.obj_str from forecast_rs a left join obj_no_list b on a.obj_no = b.obj_no"
    lines = spark.sql(sql)
    return lines


if __name__ == '__main__':
    # read data
    raw_data = get_data(spark)
    raw_data = raw_data.where("ds>'2018-06-01'")
    raw_data.persist(StorageLevel.MEMORY_AND_DISK)
    #raw_data = raw_data.where("obj_no=1 or obj_no=2 ")
    obj_size = raw_data.select("obj_no").distinct().count()
    #print(obj_size)
    forecast_date = str((datetime.date.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d"))
    #forecast_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(forecast_date)
    brdcst_cutoff_date = sc.broadcast(forecast_date)
    brdcst_forecasting_periods = sc.broadcast(90)

    forecast_rs = raw_data.rdd.map(
        lambda x: (x.obj_no, x)).partitionBy(
        obj_size, self_partition).mapPartitionsWithIndex(
        back_testing)
    col_datatype = [StructField(OBJ_NO, IntegerType(), False)] + \
                   [StructField(name, DoubleType(), False) for name in [Y_HAT,Y]] + \
                   [StructField(DATETIME_STAMP, StringType(), False)]
    schema = StructType(col_datatype)
    forecast_df = spark.createDataFrame(forecast_rs, schema)

    # 处理结果
    data_rs = deal_result(forecast_df)
    data_rs.persist(StorageLevel.MEMORY_AND_DISK)
    #data_rs = data_rs.repartition(1)
    data_rs.createOrReplaceTempView("data_rs")
    sql="select	tt.forecast_dt,	null as region_id,	tt.region_name,	tt.seller_no,	seller.seller_name,	tt.dept_no,	dept.dept_name,	tt.warehouse_no as wh_store_no,	warehouse.warehouse_name as wh_store_name,	round(tt.forecast_value *(		case			when tt.rate is null			then 0			else tt.rate		end), 0) as forecast_value,	round(		case			when c.y is null			then 0			else c.y		end, 0) as real_value,	round((tt.forecast_value *(		case			when tt.rate is null			then 0			else tt.rate		end) -(		case			when c.y is null			then 0			else c.y		end)), 0) as bias from	(		select			a.forecast_dt,			a.region_name,			a.seller_no,			a.dept_no,			b.warehouse_no,			b.rate,			a.forecast_value		from			(				select					ds as forecast_dt,					split(obj_str, '_') [0] as region_name,					split(obj_str, '_') [1] as seller_no,					split(obj_str, '_') [2] as dept_no,					yhat as forecast_value,					y as real_value				from					data_rs			)			a		left join			(				select * from rate_data			)			b		on			a.region_name = b.region_name			and a.seller_no = b.seller_no			and a.dept_no = b.dept_no	)	tt left join	(		select			ds,			region_name,			seller_no,			dept_no,			warehouse_no,			sum(y) as y		from			wh_data		group by			ds,			region_name,			seller_no,			dept_no,			warehouse_no	)	c on	tt.forecast_dt = c.ds	and tt.warehouse_no = c.warehouse_no	and tt.region_name = c.region_name	and tt.seller_no = c.seller_no	and tt.dept_no = c.dept_no left join	(		select			id,			seller_no,			seller_name		from			fdm.fdm_eclp_master_seller_chain		where			dp = 'ACTIVE'			and start_date <= '" + dt + "'			and end_date >= '" + dt + "'	)	seller on	tt.seller_no = seller.seller_no left join	(		select			dept_no,			dept_name,			seller_id		from			fdm.fdm_eclp_master_dept_chain		where			dp = 'ACTIVE'			and start_date <= '" + dt + "'			and end_date >= '" + dt + "'	)	dept on	tt.dept_no = dept.dept_no left join	(		select			*		from			fdm.fdm_eclp_master_warehouse_chain		where			dp = 'ACTIVE'			and yn = 1			and start_date <= '" + dt + "'			and end_date >= '" + dt + "'	)	warehouse on	tt.warehouse_no = warehouse.warehouse_no "
    data_rs_combined = spark.sql(sql)
    data_rs_combined = data_rs_combined.repartition(1)
    data_rs_combined.createOrReplaceTempView("data_rs_combined")

    spark.sql(
        "insert overwrite table app.app_vsc_ord_forecast_seller_output_backtest partition("
        "dt='" + forecast_date + "') select * from data_rs_combined"
         )
