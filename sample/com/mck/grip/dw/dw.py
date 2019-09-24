
from pyspark.sql.session import SparkSession
# from pandas._libs.join import inner_join
from pyspark.sql.functions import expr
from pyspark.sql import functions as F
from pyspark.sql.functions import concat
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars file:///C:/Python27/Lib/site-packages/pyspark/jars/mysql-connector-java-5.1.45-bin.jar'
# import com.mck.grip.helper.helper as h

# spark = SparkSession.builder.appName("Rules").config("spark.driver.extraClassPath", "C:/Users/HArsh/Desktop/mysql-connector-java-5.1.48/mysql-connector-java-5.1.48-bin.jar").getOrCreate()

spark = SparkSession.builder.appName("First").config("spark.driver.extraClassPath", "C:\Users\komalt\Downloads\spark-2.4.4-bin-hadoop2.7\spark-2.4.4-bin-hadoop2.7\spark-2.4.4-bin-hadoop2.7\python\lib\mysql-connector-java-5.1.48-bin.jar").getOrCreate()


def create_SHA():
    source_table = 'form_sch_a_'
    source_table_2 = 'form_sch_a_part1'
    year = 2017
    source_sch_a = concat(source_table, year)
    source_sch_a_part1 = concat(source_table_2, year)
    
    # Class.forName("com.jdbc.mysql.Driver")1
    df_sha_a = spark.read.format("jdbc").options(url="jdbc:mysql://localhost:3306/hdmnifitest1_ods", driver='com.mysql.jdbc.Driver', dbtable=source_sch_a, user="root" , password="sqluser12345").load()
    df_sha_part1 = spark.read.format("jdbc").options(url="jdbc:mysql://localhost:3306/hdmnifitest1_ods", driver='com.mysql.jdbc.Driver', dbtable=source_sch_a_part1, user="root" , password="sqluser12345").load()
    
    sha = df_sha_a.alias('sha')
    sha_1 = df_sha_part1.alias('sha_1') 
    df_join = sha.join(sha_1, (sha.ack_id == sha_1.ack_id) & (sha.form_id == sha_1.form_id), 'inner')
    df_join.show()
    
    
def preparebasefiling(): 
    
    df_form_5500 = spark.read.format("jdbc").options(url="jdbc:mysql://localhost:3306/hdmnifitest1_ods", driver='com.mysql.jdbc.Driver', dbtable="form_5500_2017", user="root" , password="sqluser12345").load()
    df111 = df_form_5500.select("*", expr("case when (TOT_ACT_RTD_SEP_BENEF_CNT = 0 or TOT_ACT_RTD_SEP_BENEF_CNT is null) then (RTD_SEP_PARTCP_RCVG_CNT+RTD_SEP_PARTCP_FUT_CNT+SUBTL_ACT_RTD_SEP_CNT+BENEF_RCVG_BNFT_CNT) else TOT_ACT_RTD_SEP_BENEF_CNT end as Total_part"),
                                expr("if (case when (case when upper(sponsor_dfe_name) rlike 'HEALTH.*|MEDICAL.*' = true then 1 else  0 end =0)=0 \
       and (case when upper(plan_name) rlike 'HEALTH.*|MEDICAL.*|COMPREHENSIVE.*'  = true then 1 else 0 end \
           )=1 then 1 \
         end = 1 , 1 ,0 \
     ) as medical_pn"),
                                expr("case when SPONS_SIGNED_NAME = '' and DFE_SIGNED_NAME!='' then DFE_SIGNED_NAME \
     when SPONS_SIGNED_NAME = '' and SPONS_MANUAL_SIGNED_NAME!='' then SPONS_MANUAL_SIGNED_NAME else SPONS_SIGNED_NAME \
  end as signed_name"),
                                expr("case when (case when ADMIN_SIGNED_NAME ='' and Admin_MANUAL_SIGNED_NAME!='' then Admin_MANUAL_SIGNED_NAME else ADMIN_SIGNED_NAME end)='' \
       and (case when SPONS_SIGNED_NAME = '' and DFE_SIGNED_NAME!='' then DFE_SIGNED_NAME \
     when SPONS_SIGNED_NAME = '' and SPONS_MANUAL_SIGNED_NAME!='' then SPONS_MANUAL_SIGNED_NAME else SPONS_SIGNED_NAME \
  end)!='' \
  and (if (SPONS_DFE_MAIL_US_ADDRESS1= ADMIN_US_ADDRESS1,1,0))=1 then case when SPONS_SIGNED_NAME = '' and DFE_SIGNED_NAME!='' then DFE_SIGNED_NAME \
     when SPONS_SIGNED_NAME = '' and SPONS_MANUAL_SIGNED_NAME!='' then SPONS_MANUAL_SIGNED_NAME else SPONS_SIGNED_NAME \
  end \
  else \
  case when ADMIN_SIGNED_NAME ='' and Admin_MANUAL_SIGNED_NAME!='' then Admin_MANUAL_SIGNED_NAME else ADMIN_SIGNED_NAME end \
  end as admin"),
  expr("if (SPONS_DFE_MAIL_US_ADDRESS1= ADMIN_US_ADDRESS1,1,0) as admin_is_group"),
  expr("concat(SPONS_DFE_LOC_US_STATE,SPONS_DFE_EIN,SPONS_DFE_PN) as EINPN"))
