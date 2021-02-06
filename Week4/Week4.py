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
if __name__ == '__main__':
    csv_path = 'test/Oregon Hwy 26 Crash Data for 2019 - Crashes on Hwy 26 during 2019.csv'
    df = pd.read_csv(csv_path)
    #print(type(df))
    #print(df.head(10))
    CrashesDF = df[df['Record Type'] == 1]
    VehiclesDF = df[df['Record Type'] == 2]
    ParticipantsDF = df[df['Record Type'] == 3]

    CrashesDF = CrashesDF.dropna(axis=1, how='all')
    VehiclesDF = VehiclesDF.dropna(axis=1, how='all')
    ParticipantsDF = ParticipantsDF.dropna(axis=1, how='all')
    #print(CrashesDF.head(10))
    #print(VehiclesDF.head(10))
    #print(ParticipantsDF.head(10))

    #To get a sum of a group
    count_list=CrashesDF.groupby("Crash Month").size();
    list=count_list.tolist()
    print(list)
    #plt.title("Month -Crashe")
    #plt.xlabel("Month")
    #plt.ylabel("Total crashes")
    #plt.plot(months,list,color="blue", linewidth=1.0, linestyle="-")

    #plt.show()
    sum_of_vehicle=df[["Total Vehicle Count","Crash Month"]].groupby("Crash Month").sum()
    #sum_of_vehicle.plot()
    #print(sum_of_vehicle)
    #print(type(sum_of_vehicle))
    #ax = sns.distplot(sum_of_vehicle, hist=True, kde=True, rug=False, color='m', bins=25, hist_kws={'edgecolor': 'black'})
    #plt.show()

    ErrorDF = CrashesDF[df['Record Type'] == 1]

    #print(CrashesDF["Crash Year"].describe())
    print(CrashesDF["Week Day Code"].describe())
    print(CrashesDF["Crash Hour"].describe())
    print(CrashesDF["Longitude Minutes"].describe())
    print(CrashesDF["Total Fatality Count"].describe())
    print(CrashesDF["Weather Condition"].describe())
    print(CrashesDF["Road Surface Condition"].describe())
    print(CrashesDF["Light Condition"].describe())
    print(ParticipantsDF["Sex"].describe())
    print(ParticipantsDF["Age"].describe())
    mon_list=CrashesDF["Crash Month"].tolist()
    day_list=CrashesDF["Crash Day"].tolist()
    
    #get the total day of the year
    total_days=[]
    for i in range(len(day_list)):
        days_count=0;
        for j in range(0,months.index(mon_list[i])):
            days_count+=days[j]
        days_count+=day_list[i]
        #print(days_count)
        total_days.append(days_count)

    #change the ages of participant
    ages = ParticipantsDF["Age"].tolist()
    for i in range(len(ages)):
        if (ages[i] == 0):
            ages[i] = "00"
        elif (ages[i] == 2):
            ages[i] = str(20 + random.randint(1, 20))
        elif (ages[i] == 4):
            ages[i] = str(40 + random.randint(1, 20))
        elif (ages[i] == 6):
            ages[i] = str(60 + random.randint(1, 38))
        elif (ages[i] == 9):
            ages[i] = "99"
        else:
            ages[i] = "00"
    print(ages)
    ParticipantsDF["Age"] = ages
    CrashesDF['Total Days']=total_days
    
    #If the road surface condition is out of range, change the conditions
    conditions=CrashesDF["Road Surface Condition"].tolist()
    for i in range(len(conditions)):
        if conditions[i] not in range(0,4):
            if (conditions[i]==99):
                conditions[i]=0
            else:
                conditions[i]=random.randint(0,4)
    print(conditions)
    CrashesDF["Road Surface Condition"]=conditions

    crash_hour=CrashesDF["Crash Hour"].tolist()
    light_conditions=[]
    #change the light conditions by crash hour:
    #5-7:Dawn 17-19:Dusk 7-17:Daylight 99:Unkonw else:random 2,3
    for i in range(len(crash_hour)):
        if(crash_hour[i]==99):
            light_conditions.append(0)
        elif crash_hour[i] in range(5,7):
            light_conditions.append(4)
        elif crash_hour[i] in range(17,19):
            light_conditions.append(5)
        elif crash_hour[i] in range(7,17):
            light_conditions.append(1)
        else:
            light_conditions.append(random.randint(2, 3))
    print(light_conditions)
    CrashesDF["Light Condition"]=light_conditions
    #print(total_days)
    #eachday_of_vehicle = CrashesDF[["Total Vehicle Count", "Total Days"]].groupby("Total Days").sum()
    #people_age=ParticipantsDF[["Age"]]
    #print(eachday_of_vehicle)
    #eachday_of_vehicle.plot()
    #ax = sns.distplot(eachday_of_vehicle, hist=True, kde=True, rug=False, color='m', bins=25, hist_kws={'edgecolor': 'black'})
    #ax = sns.distplot(people_age, hist=True, kde=True, rug=False, color='m', bins=25,hist_kws={'edgecolor': 'black'})
    #ax = sns.distplot(CrashesDF[["Road Surface Condition"]], hist=True, kde=True, rug=False, color='m', bins=25, hist_kws={'edgecolor': 'black'})
    ax=sns.distplot(CrashesDF[["Light Condition"]], hist=True, kde=True, rug=False, color='m', bins=25, hist_kws={'edgecolor': 'black'})
    plt.show()


