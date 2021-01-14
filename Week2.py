import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

from pylab import rcParams
from urllib.request import urlopen
from bs4 import BeautifulSoup



if __name__ == '__main__':

    url = "http://www.hubertiming.com/results/2017GPTR10K"
    html = urlopen(url)
    soup = BeautifulSoup(html, 'lxml')

    #get the type of bs4 object
    print(type(soup))
    #get the title of this page
    title = soup.title
    print(title)
    #get the text of this page
    text=soup.get_text
    #print(text)

    #how to use find_all() function
    all_links=soup.find_all('a')
    #for link in all_links:
        #print(link.get('href'))
    rows=soup.find_all('tr')
    #print(rows[:10])
    for row in rows:
        row_td=row.find_all('td')
    #print(row_td)
    #type(row_td)

    #how to remove html tags
    str_cells=str(row_td)
    cleantext=BeautifulSoup(str_cells,'lxml').get_text()
    #print(cleantext)

    #get cleantext use regex
    list_rows=[]
    for row in rows:
        cells=row.find_all('td')
        str_cells=str(cells)
        regex=re.compile('<.*?>')
        clean_text=(re.sub(regex,'',str_cells))
        list_rows.append(clean_text)
    #print(clean_text)
    #print(type(clean_text))
    """"
        use '<.*>' regex
        [LIBBY B MITCHELL ]
        use '<.*?>' regex
[577, 443,  LIBBY B MITCHELL, F, HILLSBORO, OR, 1:41:18, 16:20, 1:42:10, ]
        
   """""

    #how to use pd
    df=pd.DataFrame(list_rows)
    #print(df.head(10))

    #devide rows
    #about the str.split function
    #https: // pandas.pydata.org / pandas - docs / stable / reference / api / pandas.Series.str.split.html
    df1=df[0].str.split(',',expand=True)
    df1.head(10)
    #print(df1.head(10))

    #get headers
    col_labels=soup.find_all('th')
    all_header=[]
    col_str=str(col_labels)
    headers=BeautifulSoup(col_str,'lxml').get_text()
    all_header.append(headers)
    #print(all_header)
    df2=pd.DataFrame(all_header)
    #print(df2.head())

    #devide headers
    df3=df2[0].str.split(',',expand=True)
    #print(df3.head())

    #concatenate two tables into one
    new_table=[df3,df1]
    df4=pd.concat(new_table)
    #print(df4.head(10))

    #use first row as the table header
    #rename function
    #https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rename.html
    df5=df4.rename(columns=df4.iloc[0])
    #print(df5.head())
    #print(df5.info())
    #print(df5.shape)
    """ dtypes: object(10)
        memory usage: 50.1+ KB
        None
        (583, 10)        it seems works XD
                                <th>Place</th>
                                <th>Bib</th>
                                <th>Name</th>
                                <th>Gender</th>
                                <th>City</th>
                                <th>State</th>
                                <th>Chip Time</th>
                                <th>Chip Pace</th>
                                <th>Gun Time</th>
                                <th>Team</th>
    """
    #drop NA values
    #dropna function drop NA values
    #https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.dropna.html
    df6=df5.dropna()
    #print(df6.info())
    #print(df6.shape)

    #drop rows or cols
    df7=df6.drop(df6.index[0])
    #print(df7.head())

    #use rename to clean up the headers
    df7.rename(columns={'[Place':'Place'},inplace=True)
    df7.rename(columns={' Team]':'Team'},inplace=True)
    #print(df7.head())

    #removing ']] in Team
    df7['Team']=df7['Team'].str.strip(']')
    #print(df7.head())

    #average finish time
    time_list = df7[' Chip Time'].tolist()
    time_mins = []
    for i in time_list:
        num = i.split(':')
        #the len of num maybe 3 or 2
        if len(num)==3:
            math = (int(num[0]) * 3600 + int(num[1])*60+int(num[2])) / 60
        if len(num)==2:
            math = (int(num[0]) * 60 + int(num[1])) / 60
        time_mins.append(math)
    #print(len(time_mins))
    #print(time_mins)

    #convert the new list
    df7['Runner_mins'] = time_mins
    #print(df7.head())

    #show details of Runner_mins
    #print(df7.describe(include=[np.number]))

    #data summary statistics for the runners
    rcParams['figure.figsize'] = 15, 5

    df7.boxplot(column='Runner_mins')
    plt.grid(True, axis='y')
    plt.ylabel('Chip Time')
    plt.xticks([1], ['Runners'])
    #plt.show()
    plt.clf()
    plt.cla()

    #Runner_mins' ditribution
    x = df7['Runner_mins']
    ax = sns.distplot(x, hist=True, kde=True, rug=False, color='m', bins=25, hist_kws={'edgecolor': 'black'})
    #plt.show()
    plt.clf()
    plt.cla()

    #the perfromance differences between males and femals
    f_fuko = df7.loc[df7[' Gender'] == ' F']['Runner_mins']
    m_fuko = df7.loc[df7[' Gender'] == ' M']['Runner_mins']
    sns.distplot(f_fuko, hist=True, kde=True, rug=False, hist_kws={'edgecolor': 'black'}, label='Female')
    sns.distplot(m_fuko, hist=False, kde=True, rug=False, hist_kws={'edgecolor': 'black'}, label='Male')
    plt.legend()
    #plt.show()
    plt.clf()
    plt.cla()

    #the summary
    g_stats = df7.groupby(" Gender", as_index=True).describe()
    #print(g_stats)

    #average chip time
    df7.boxplot(column='Runner_mins', by=' Gender')
    plt.ylabel('Chip Time')
    #plt.suptitle("")
    plt.show()










