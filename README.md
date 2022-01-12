# Trade quantity estimation
## Background
International Merchandise Trade Statistics often suffer from poor quantity values. One of the main reasons is that recording quantity information is not given the same importance as recording trade values. The main source of trade data is customs. Since taxes and duties are determined based on the values and not the quantities, there's often no incentive of recording them diligently.

This leaves the statisticians with less than perfect data. Since many analyses require quantities and/or unit values, quantity information is of interest to the statisticians and policy makers.

The objective of the joint UNSD-ITC-FAO project is to detect outliers in trade quantities and estimate missing/outliers values.

## Data set
As a test case, data on rice imports by Mozambique between the years 2000 and 2019 are used.

## Tools
The volume of trade statistics necessitates use of cloud computing. For this project Azure databricks was used. The code was written in Python (PySpark).

## Algorithm
**Extract data**   
Mozambique rice imports annual data (df) for all partners between 2000-2019.

```
df = sqlContext.sql("select reporterCode, partnerCode, period, cmdCode, primaryValue, netWgt, qtyUnitCode, altQtyUnitCode, altQty from delta.dtarifflineannual where reporterCode = '508' and flowCode = 'M' and cmdCode like '100630%' and period BETWEEN 2000 AND 2019")
```
![df-after-extraction](https://github.com/HabibRKhan/trade_quantity_estimation/blob/main/df1.PNG)

**Import SUV***   
In the initial round, outliers are removed using the Statndard Unit Values (SUV) table downloaded from COMTRADE <https://unstats.un.org/unsd/trade/data/tables.asp#SUV>.   
The table was previously uploaded to databricks Filestore which are then extracted.

```
SUV = pd.read_csv("/dbfs/FileStore/tables/TSUV_tabDelimUtf.txt", sep="\t")

#Subset the SUV table to keep only imports(flow_code=M) of rice(commodity_group_code=100630)
SUV = SUV[(SUV.trade_flow=="M") & (SUV.commodity_group_code==100630)]
```
![df-after-extraction](https://github.com/HabibRKhan/trade_quantity_estimation/blob/main/SUV.PNG)

**Keep SUVs for only latest HS version**    
Only necessary columns in SUV are kept and prefix H from commodity classification version is dropped. The version number is then used to sort and remove duplicates keeping only the latest version.

```
SUV = SUV[['year', 'commodity_group_code', 'commodity_classification', 'value']]
SUV['commodity_classification'] = SUV['commodity_classification'].str[1:2]

#Now let's sort on year, commodity code and classification version, and then drop duplicates based on year and commodity by keeping last record. This will make sure that the record with the latest classification version remains.
SUV = SUV.sort_values(by=['year', 'commodity_group_code', 'commodity_classification'])
SUV = SUV.drop_duplicates(subset = ['year', 'commodity_group_code'], keep = 'last')
```
![df-after-extraction](https://github.com/HabibRKhan/trade_quantity_estimation/blob/main/SUV2.PNG)

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
![df-after-extraction](https://github.com/HabibRKhan/trade_quantity_estimation/blob/main/df_SUV.PNG)

**Estimate missing netweights**   
If netWgt is null, it will be estimated as primaryValue/value (SUV). But before that, create a flag variable which will be 1 if net weight is estimated, 0 otherwise. Next, if qtyUnitCode is -1, force it to 8.

```
df['flag'] = np.where(df['netWgt'].isnull(), 1, 0)
df['netWgt_complete'] = np.where(df['netWgt'].isnull(), (df['primaryValue']/df['SUV']), df['netWgt'])
df['qtyUnitCode'] = pd.to_numeric(df['qtyUnitCode'])
df['qtyUnitCode_complete'] = np.where((df['qtyUnitCode']==-1), 8, df['qtyUnitCode'])
```

**Calulate ratio of available records***   
First create a table with partner, commodity and period; and then count how many periods by that combination are there. 

```
count_series = df.groupby(['partnerCode', 'commodity_group_code', 'period']).size()
freq_df = count_series.to_frame(name = 'size').reset_index()
```

Many partners have multiple records per period. But we're interested in the ratio of number of periods for which records are available to total periods.

```
freq_df = freq_df.drop(['size'], axis = 1)
freq_df = freq_df.groupby(['partnerCode', 'commodity_group_code']).size()
freq_df = freq_df.to_frame(name = 'nperiod').reset_index()
```

Finally, calculate the ratio by dividing by total number of periods.

```
n = df['period'].nunique()
freq_df['share'] = freq_df['nperiod']/n
```
![df-after-extraction](https://github.com/HabibRKhan/trade_quantity_estimation/blob/main/Freq.PNG)

Very few partners have records for more than 80% of the periods.

```
freq_df.hist(column="share", bins=5, grid=False, rwidth=.97)
```
![df-after-extraction](https://github.com/HabibRKhan/trade_quantity_estimation/blob/main/Freq_hist.PNG)

But these partners account for majority of values.

```
group1 = freq_df[freq_df["share"]>=0.8]["partnerCode"]
group2 = freq_df[freq_df["share"]<0.8]["partnerCode"]

sum1 = df[df["partnerCode"].isin(group1)]["primaryValue"].sum()
sum2 = df[df["partnerCode"].isin(group2)]["primaryValue"].sum()

pie_df = pd.DataFrame({'partner_group': ["more_than_80%", "less_than_80%"],
                   'sum_values': [sum1, sum2]})

pie_df.plot(kind='pie', y='sum_values')
```
![df-after-extraction](https://github.com/HabibRKhan/trade_quantity_estimation/blob/main/Freq_pie.PNG)

**Calculate new variables for Z score**   
