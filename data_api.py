import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
import argparse

sc = SparkContext(appName="myAppName")
sqlContext = SQLContext(sc)


class mobilityAPI:

    
    def __init__(self, date=None, country=None, state=None, county=None, mobilitySelector=None):
        
        self.date = date
        self.country = country
        self.state = state
        self.county = county
        self.mobilitySelector = mobilitySelector

    
    def execute(self):
        
        sparkDF = self.buildSpark()
        queriedDF = self.queryBuilder(sparkDF)
        self.outputData(queriedDF)
        self.outputGraph(queriedDF)
        return

    
    def buildSpark(self):
        
        schema = StructType([StructField("country_region_code", StringType(), True),
                             StructField("country_region", StringType(), True),
                             StructField("sub_region_1", StringType(), True),
                             StructField("sub_region_2", StringType(), True),
                             StructField("date", DateType(), True),
                             StructField("retail_and_recreation_percent_change_from_baseline", IntegerType(), True),
                             StructField("grocery_and_pharmacy_percent_change_from_baseline", IntegerType(), True),
                             StructField("parks_percent_change_from_baseline", IntegerType(), True),
                             StructField("transit_stations_percent_change_from_baseline", IntegerType(), True),
                             StructField("workplaces_percent_change_from_baseline", IntegerType(), True),
                             StructField("residential_percent_change_from_baseline", IntegerType(), True)])

        sparkDF = sqlContext.read.csv("mobility.csv", header=True, dateFormat="MM/dd/yy", mode="DROPMALFORMED", schema=schema)

        return sparkDF


    def buildPandas(self, sparkDF):

        pandasDF = sparkDF.select("*").toPandas()

        return pandasDF

    
    def queryBuilder(self, sparkDF):

        filteredDF = sparkDF
        preSelections = []

        preSelections.append('date')
        if(self.date != None):
            filteredDF = filteredDF.filter(filteredDF['date'] == self.date)

        if(self.country != None):
            preSelections.append('country_region_code')
            filteredDF = filteredDF.filter(filteredDF['country_region_code'] == self.country)
            if(self.state != None):
                preSelections.append('sub_region_1')
                filteredDF = filteredDF.filter(filteredDF['sub_region_1'] == self.state)
                if(self.county != None):
                    preSelections.append('sub_region_2')
                    filteredDF = filteredDF.filter(filteredDF['sub_region_2'] == self.county)
                else:
                    filteredDF = filteredDF.where(col("sub_region_2").isNull())
            else:
                filteredDF = filteredDF.where(col("sub_region_1").isNull())
                filteredDF = filteredDF.where(col("sub_region_2").isNull())
        
        else:
            preSelections.append('country_region_code')
            filteredDF = filteredDF.where(col("sub_region_1").isNull())
            filteredDF = filteredDF.where(col("sub_region_2").isNull())

        if(self.mobilitySelector != None):
            switch = {
                1: ['retail_and_recreation_percent_change_from_baseline',
                    'grocery_and_pharmacy_percent_change_from_baseline',
                    'parks_percent_change_from_baseline',
                    'transit_stations_percent_change_from_baseline',
                    'workplaces_percent_change_from_baseline',
                    'residential_percent_change_from_baseline'],
                2: ['retail_and_recreation_percent_change_from_baseline'],
                3: ['grocery_and_pharmacy_percent_change_from_baseline'],
                4: ['parks_percent_change_from_baseline'],
                5: ['transit_stations_percent_change_from_baseline'],
                6: ['workplaces_percent_change_from_baseline'],
                7: ['residential_percent_change_from_baseline']
            }
            optimize = switch.get(int(self.mobilitySelector), "Invalid")
            concat = preSelections + optimize
            filteredDF = filteredDF.select([x for x in filteredDF.columns if x in concat])
        else:
            switch = {
                1: ['retail_and_recreation_percent_change_from_baseline',
                    'grocery_and_pharmacy_percent_change_from_baseline',
                    'parks_percent_change_from_baseline',
                    'transit_stations_percent_change_from_baseline',
                    'workplaces_percent_change_from_baseline',
                    'residential_percent_change_from_baseline'],
                2: ['retail_and_recreation_percent_change_from_baseline'],
                3: ['grocery_and_pharmacy_percent_change_from_baseline'],
                4: ['parks_percent_change_from_baseline'],
                5: ['transit_stations_percent_change_from_baseline'],
                6: ['workplaces_percent_change_from_baseline'],
                7: ['residential_percent_change_from_baseline']
            }
            optimize = switch.get(1, "Invalid")
            concat = preSelections + optimize
            filteredDF = filteredDF.select([x for x in filteredDF.columns if x in concat])
        return filteredDF

    
    def outputData(self, filteredDF):
        
        pandasDF = self.buildPandas(filteredDF)
        print(pandasDF)

    
    def outputGraph(self, filteredDF):

        switch = {
            1: ['retail_and_recreation_percent_change_from_baseline',
                'grocery_and_pharmacy_percent_change_from_baseline',
                'parks_percent_change_from_baseline',
                'transit_stations_percent_change_from_baseline',
                'workplaces_percent_change_from_baseline',
                'residential_percent_change_from_baseline'],
            2: ['retail_and_recreation_percent_change_from_baseline'],
            3: ['grocery_and_pharmacy_percent_change_from_baseline'],
            4: ['parks_percent_change_from_baseline'],
            5: ['transit_stations_percent_change_from_baseline'],
            6: ['workplaces_percent_change_from_baseline'],
            7: ['residential_percent_change_from_baseline']
        }
        
        pandasDF = self.buildPandas(filteredDF)

        mobility = switch.get(int(self.mobilitySelector), "Invalid")

        graph = pandasDF.set_index('date')[mobility[0]].plot(figsize=(12, 10), linewidth=2.5, color='blue', title="Deviation from Baseline")
        graph.set_xlabel("Date")
        graph.set_ylabel(mobility[0])
        output = graph.get_figure()
        output.savefig('graph.png')

        return



    
parser = argparse.ArgumentParser()
parser.add_argument('--date', default=None)
parser.add_argument('--country', default=None)
parser.add_argument('--state', default=None)
parser.add_argument('--county', default=None)
parser.add_argument('--mobilitySelector', default=None)
args = parser.parse_args()

obj = mobilityAPI(args.date, args.country, args.state, args.county, args.mobilitySelector)
obj.execute()

    

    