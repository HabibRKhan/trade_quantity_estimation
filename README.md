# Trade quantity estimation
## Background
International Merchandise Trade Statistics often suffer from poor quantity values. One of the main reasons is that recording quantity information is not given the same importance as recording trade values. The main source of trade data is customs. Since taxes and duties are determined based on the values and not the quantities, there's often no incentive of recording them diligently.

This leaves the statisticians with less than perfect data. Since many analyses require quantities and/or unit values, quantity information is of interest to the statisticians and policy makers.

The objective of the joint UNSD-ITC-FAO project is to detect outliers in trade quantities and estimate missing/outliers values.

## Data set
As a test case, data on rice imports by Mozambique between the years 2000 and 2019 are used.

## Tools
The volume of trade statistics necessitates use of cloud computing. For this project Azure databricks was used. The code was written in Python (PySpark).

## Analyses
**Extract data**   
Mozambique rice imports annual data (df) for all partners between 2000-2019.

```
df = sqlContext.sql("select reporterCode, partnerCode, period, cmdCode, primaryValue, netWgt, qtyUnitCode, altQtyUnitCode, altQty from delta.dtarifflineannual where reporterCode = '508' and flowCode = 'M' and cmdCode like '100630%' and period BETWEEN 2000 AND 2019")
```
**Import SUV***   
In the initial round, outliers are removed using the Statndard Unit Values (SUV) table downloaded from COMTRADE <https://unstats.un.org/unsd/trade/data/tables.asp#SUV>.   
The table was previously uploaded to databricks Filestore which are then extracted.

```
SUV = pd.read_csv("/dbfs/FileStore/tables/TSUV_tabDelimUtf.txt", sep="\t")

#Subset the SUV table to keep only imports(flow_code=M) of rice(commodity_group_code=100630)
SUV = SUV[(SUV.trade_flow=="M") & (SUV.commodity_group_code==100630)]
```

**Keep SUVs for only latest HS version**    
Only necessary columns in SUV are kept and prefix H from commodity classification version is dropped. The version number is then used to sort and remove duplicates keeping only the latest version.

```
SUV = SUV[['year', 'commodity_group_code', 'commodity_classification', 'value']]
SUV['commodity_classification'] = SUV['commodity_classification'].str[1:2]

#Now let's sort on year, commodity code and classification version, and then drop duplicates based on year and commodity by keeping last record. This will make sure that the record with the latest classification version remains.
SUV = SUV.sort_values(by=['year', 'commodity_group_code', 'commodity_classification'])
SUV = SUV.drop_duplicates(subset = ['year', 'commodity_group_code'], keep = 'last')
```

**Merge df with SUV**   
SUV is merged with df by year and commodity code. We need to rename SUV's year column as period and create a column in df with 6-digits commodity codes named as commodity_group_code. This ensures that the two dataframes have two columns each with same titles to merge on. Also, renaming the SUV column from value to SUV to avoid confusion with primaryValue in df

```
SUV.rename(columns = {'year':'period',
                     'value': 'SUV'},
           inplace = True)
SUV['commodity_group_code'] = SUV['commodity_group_code'].astype(str)
df['commodity_group_code'] = df['cmdCode'].str[:6]
df = pd.merge(df, SUV, how='left')
```

**Estimate missing netweights as value/SUV, put a flag**   
**Calulate ratio of available records by period for each partner to total number of periods**   
**Calculate new variables for Z score**   
