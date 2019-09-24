#from com.mck.grip.dw.dw import preparebasefiling
from com.mck.grip.exception.exception import DataBaseException
from pyspark.sql.session import SparkSession
import com.mck.grip.dw.dw as d
#import com.mck.grip.helper.helper.Helperclass as h

#import pandas as pd


def main():
    #("abc", "abc", "abc","abc")
    #obj2 = obj.connectionpool()
    #d.preparebasefiling()
    #h.connectionpool()
    #obj2.createDataFrame()
    #d.preparebasefiling(obj2.get_values().Input_table, obj2.get_values().Output_table, obj2.get_values().year)
    #connectionpool()
    d.create_SHA()
    d.preparebasefiling()
    # sqlquery_rule_step = pd.read_sql_query('select distinct modulename from rule_master', mydb)
    # df_rule_step = pd.DataFrame(sqlquery_rule_step, columns=['modulename'])

    # for module_name in list_of_module_name:
    # s.preparebasefiling()


if __name__ == "__main__":
    main()
