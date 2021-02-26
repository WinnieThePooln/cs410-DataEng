import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
import  random
from pylab import rcParams
from urllib.request import urlopen
from bs4 import BeautifulSoup
months=[1,2,3,4,5,6,7,8,9,10,11,12]
days=[31,28,31,30,31,30,31,31,30,31,30,31]
acs_features=['County','State','TotalPop','Poverty','IncomePerCap']
covid_features=['cases','deaths']
new_acs=[]
new_cvd=[]
if __name__ == '__main__':
    covid_csv_path = 'test/COVID_county_data.csv'
    acs_csv_path= 'test/acs2017_census_tract_data.csv'
    df1 = pd.read_csv(covid_csv_path)
    df2 = pd.read_csv(acs_csv_path)
    df2=df2.dropna()
    #Test the features in acs table
    #for f in acs_features:
    #    print(df2[f].describe())

    #Change the poverty from percent to population
    temp_dict=df2.to_dict('records')
    poverty=df2['Poverty'].tolist()
    totalpop=df2['TotalPop'].tolist()
    for i in range(len(poverty)):
        percent=poverty[i]/100
        poverty[i]=percent*totalpop[i]
    df2['Poverty']=poverty

    #Use groupby to get values of county in different state
    t=df2.groupby(['County','State'])
    for key,value in t:
        county=key[0]
        state=key[1]
        cnt=len(value)
        total_poverty=sum(value['Poverty'])
        total_income=sum(value['IncomePerCap'])
        total_pop=sum(value['TotalPop'])
        percent=(total_poverty/total_pop)*100
        per_income=total_income/cnt
        temp_dict={'County':county,'State':state,'TotalPop':total_pop,'Poverty':percent,'IncomePerCap':per_income}
        new_acs.insert(0,temp_dict)

    #Create new df use dict list
    new_df=pd.DataFrame(new_acs)

    print(new_df[(new_df['County']=='Loudon County')&(new_df['State']=='Tennessee')])
    print(new_df[(new_df['County']=='Washington County')&(new_df['State']=='Oregon')])
    print(new_df[(new_df['County']=='Harlan County')&(new_df['State']=='Kentucky')])
    print(new_df[(new_df['County']=='Malheur County')&(new_df['State']=='Oregon')])

    cvd=df1.groupby(['county','state'])
    for key,value in cvd:
        #print(key)
        #print(type(value))
        dec_rows=value[(value['date'].str.contains('2020-12'))]
        #print(dec_rows)
        county=key[0]
        state=key[1]
        total_case=sum(value['cases'])
        total_death=sum(value['deaths'])
        dec_case=sum(dec_rows['cases'])
        dec_death=sum(dec_rows['deaths'])
        #print(type(value['date']))
        #print(dec_case)
        #print(dec_death)
        #print(total_case)
        temp_dict={'County':county,'State':state,'Totalcase':total_case,'Totaldeath':total_death,'Deccase':dec_case,'Decdeath':dec_death}
        new_cvd.insert(0,temp_dict)

    new_cvd_df=pd.DataFrame(new_cvd)
    print(new_cvd_df[(new_cvd_df['County']=='Loudon')&(new_cvd_df['State']=='Tennessee')])
    print(new_cvd_df[(new_cvd_df['County']=='Washington')&(new_cvd_df['State']=='Oregon')])
    print(new_cvd_df[(new_cvd_df['County']=='Harlan')&(new_cvd_df['State']=='Kentucky')])
    print(new_cvd_df[(new_cvd_df['County']=='Malheur')&(new_cvd_df['State']=='Oregon')])


    print(len(new_df))
    print(len(new_cvd_df))


    acs_county=set(new_df['County'].values)
    cvd_conty=set(new_cvd_df['County'].values)

    print(len(acs_county))
    print(len(cvd_conty))

    print(len(acs_county.intersection(cvd_conty)))
    print(len(acs_county.difference(cvd_conty)))
    print(len(cvd_conty.difference(acs_county)))

    new_county_list=new_df['County'].values
    for i in range(len(new_county_list)):
        if new_county_list[i] not in acs_county.intersection(cvd_conty):
            if new_county_list[i].find(' County'):
                new_county_list[i]=new_county_list[i].replace(' County','')
                #print(new_county_list[i])
            if  new_county_list[i].find(' Municipio'):
                new_county_list[i]=new_county_list[i].replace(' Municipio','')
                #print(new_county_list[i])
            if new_county_list[i].find(' Parish'):
                new_county_list[i]=new_county_list[i].replace(' Parish','')
                #print(new_county_list[i])
            if new_county_list[i].find(' Municipio'):
                new_county_list[i]=new_county_list[i].replace(' Municipio','')
                #print(new_county_list[i])

    new_df['County']=new_county_list
    final_df = pd.merge(new_df, new_cvd_df)
    print(len(final_df))

    print(final_df[(final_df['State']=='Oregon')])

    total_mortality_rate=[]
    total_death=final_df['Totaldeath']
    total_case=final_df['Totalcase']
    total_pop = final_df['TotalPop']
    dec_case=final_df['Deccase']
    dec_death=final_df['Decdeath']
    for i in range(len(total_death)):
        temp_mr=(total_death[i]/total_case[i])*100
        total_mortality_rate.insert(0,temp_mr)

    final_df['Totalmortalityrate']=total_mortality_rate

    case_per100000=[]
    death_per100000=[]
    dec_death100000=[]
    dec_case100000=[]
    for i in range(len(total_pop)):
        case_per100000.insert(0,(total_case[i]*100000)/total_pop[i])
        death_per100000.insert(0,(total_death[i]*100000)/total_pop[i])
        dec_death100000.insert(0,(dec_death[i]*100000)/total_pop[i])
        dec_case100000.insert(0,(dec_case[i]*100000)/total_pop[i])
    final_df['CaseperOHT']=case_per100000
    final_df['DeathperOHT']=death_per100000
    final_df['DeccaseOHT']=dec_case100000
    final_df['DecdeathOHT']=dec_case100000
    #print(final_df.head(10))
    #counties in Oregon
    print(final_df[(final_df['State']=='Oregon')]['CaseperOHT'].corr(final_df[(final_df['State']=='Oregon')]['Poverty']))
    print(final_df[(final_df['State'] == 'Oregon')]['DeathperOHT'].corr(final_df[(final_df['State'] == 'Oregon')]['Poverty']))
    print(final_df[(final_df['State'] == 'Oregon')]['CaseperOHT'].corr(final_df[(final_df['State'] == 'Oregon')]['IncomePerCap']))
    print(final_df[(final_df['State'] == 'Oregon')]['DeathperOHT'].corr(final_df[(final_df['State'] == 'Oregon')]['IncomePerCap']))
    print(final_df[(final_df['State'] == 'Oregon')]['DeccaseOHT'].corr(final_df[(final_df['State'] == 'Oregon')]['Poverty']))
    print(final_df[(final_df['State'] == 'Oregon')]['DecdeathOHT'].corr(final_df[(final_df['State'] == 'Oregon')]['Poverty']))
    print(final_df[(final_df['State'] == 'Oregon')]['DeccaseOHT'].corr(final_df[(final_df['State'] == 'Oregon')]['IncomePerCap']))
    print(final_df[(final_df['State'] == 'Oregon')]['DecdeathOHT'].corr(final_df[(final_df['State'] == 'Oregon')]['IncomePerCap']))
    print(final_df[(final_df['State'] == 'Oregon')]['Totalmortalityrate'].corr(
        final_df[(final_df['State'] == 'Oregon')]['Poverty']))
    print(final_df[(final_df['State'] == 'Oregon')]['Totalmortalityrate'].corr(
        final_df[(final_df['State'] == 'Oregon')]['IncomePerCap']))
    print('-------------------------------------------------------------------------------------------------------------------')
    ax1 = final_df[(final_df['State'] == 'Oregon')].plot.scatter(x='Totaldeath',y='IncomePerCap',c='DarkBlue')
    plt.show()
    #counties in USA
    print(final_df['CaseperOHT'].corr(final_df['Poverty']))
    print(final_df['DeathperOHT'].corr(final_df['Poverty']))
    print(final_df['CaseperOHT'].corr(final_df['IncomePerCap']))
    print(final_df['DeathperOHT'].corr(final_df['IncomePerCap']))
    print(final_df['DeccaseOHT'].corr(final_df['Poverty']))
    print(final_df['DecdeathOHT'].corr(final_df['Poverty']))
    print(final_df['DeccaseOHT'].corr(final_df['IncomePerCap']))
    print(final_df['DecdeathOHT'].corr(final_df['IncomePerCap']))
    print(final_df['Totalmortalityrate'].corr(final_df['Poverty']))
    print(final_df['Totalmortalityrate'].corr(final_df['IncomePerCap']))

    print(final_df['Totalmortalityrate'].describe())










