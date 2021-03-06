#!/usr/bin/python3
# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options
import time
import psycopg2.extras
import argparse
import re
import csv
import io
DBname = "week6"
DBuser = "xiaoran"
DBpwd = "123456"
TableName = 'CensusData'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created
Year = 2015

def row2vals(row):
	# handle the null vals
	for key in row:
		if not row[key]:
			row[key] = 0
		row['County'] = row['County'].replace('\'','')  # eliminate quotes within literals
	ret = f"""
       {Year},                          -- Year
       {row['CensusTract']},            -- CensusTract
       '{row['State']}',                -- State
       '{row['County']}',               -- County
       {row['TotalPop']},               -- TotalPop
       {row['Men']},                    -- Men
       {row['Women']},                  -- Women
       {row['Hispanic']},               -- Hispanic
       {row['White']},                  -- White
       {row['Black']},                  -- Black
       {row['Native']},                 -- Native
       {row['Asian']},                  -- Asian
       {row['Pacific']},                -- Pacific
       {row['Citizen']},                -- Citizen
       {row['Income']},                 -- Income
       {row['IncomeErr']},              -- IncomeErr
       {row['IncomePerCap']},           -- IncomePerCap
       {row['IncomePerCapErr']},        -- IncomePerCapErr
       {row['Poverty']},                -- Poverty
       {row['ChildPoverty']},           -- ChildPoverty
       {row['Professional']},           -- Professional
       {row['Service']},                -- Service
       {row['Office']},                 -- Office
       {row['Construction']},           -- Construction
       {row['Production']},             -- Production
       {row['Drive']},                  -- Drive
       {row['Carpool']},                -- Carpool
       {row['Transit']},                -- Transit
       {row['Walk']},                   -- Walk
       {row['OtherTransp']},            -- OtherTransp
       {row['WorkAtHome']},             -- WorkAtHome
       {row['MeanCommute']},            -- MeanCommute
       {row['Employed']},               -- Employed
       {row['PrivateWork']},            -- PrivateWork
       {row['PublicWork']},             -- PublicWork
       {row['SelfEmployed']},           -- SelfEmployed
       {row['FamilyWork']},             -- FamilyWork
       {row['Unemployment']}            -- Unemployment
	"""
	return ret


def initialize():
  global Year

  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  parser.add_argument("-y", "--year", default=Year)
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable
  Year = args.year

# read the input data file into a list of row strings
# skip the header row
def readdata(fname):
	print(f"readdata: reading from File: {fname}")
	with open(fname, mode="r") as fil:
		dr = csv.DictReader(fil)
		headerRow = next(dr)
		# print(f"Header: {headerRow}")

		rowlist = []
		for row in dr:
			rowlist.append(row)
	return rowlist

# convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLcmnds(rowlist):
	cmdlist = []
	for row in rowlist:
		valstr = row2vals(row)
		cmd = f"INSERT INTO {TableName} VALUES ({valstr});"
		cmdlist.append(cmd)
	return cmdlist
# connect to the database
def dbconnect():
	connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
        )
	connection.autocommit = False
	temp_file_limit=256
	return connection
        

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

	with conn.cursor() as cursor:
		cursor.execute(f"""
        	DROP TABLE IF EXISTS {TableName};
        	CREATE TEMP TABLE {TableName} (
            	Year                INTEGER,
                CensusTract         NUMERIC,
            	State               TEXT,
            	County              TEXT,
            	TotalPop            INTEGER,
            	Men                 INTEGER,
            	Women               INTEGER,
            	Hispanic            DECIMAL,
            	White               DECIMAL,
            	Black               DECIMAL,
            	Native              DECIMAL,
            	Asian               DECIMAL,
            	Pacific             DECIMAL,
            	Citizen             DECIMAL,
            	Income              DECIMAL,
            	IncomeErr           DECIMAL,
            	IncomePerCap        DECIMAL,
            	IncomePerCapErr     DECIMAL,
            	Poverty             DECIMAL,
            	ChildPoverty        DECIMAL,
            	Professional        DECIMAL,
            	Service             DECIMAL,
            	Office              DECIMAL,
            	Construction        DECIMAL,
            	Production          DECIMAL,
            	Drive               DECIMAL,
            	Carpool             DECIMAL,
            	Transit             DECIMAL,
            	Walk                DECIMAL,
            	OtherTransp         DECIMAL,
            	WorkAtHome          DECIMAL,
            	MeanCommute         DECIMAL,
            	Employed            INTEGER,
            	PrivateWork         DECIMAL,
            	PublicWork          DECIMAL,
            	SelfEmployed        DECIMAL,
            	FamilyWork          DECIMAL,
            	Unemployment        DECIMAL
         	);	
         	ALTER TABLE {TableName} ADD PRIMARY KEY (Year,CensusTract);
                CREATE INDEX idx_{TableName}_State ON {TableName}(State);	
    	""")
                #D:Delete CREATE INDEX
                #E:ALTER TABLE "tablename" SET UNLOGGED
                #  ALTER TABLE "tablename" SET LOGGED
                #F: CREATE TEMP TABLE "tablename"

		print(f"Created {TableName}")

def load(conn, icmdlist,rlist):

	with conn.cursor() as cursor:
		print(f"Loading {len(icmdlist)} rows")
		start = time.perf_counter()
		csv_object=io.StringIO()
		print(type(rlist))
		for v in rlist:
			tup=(str(v['CensusTract']),str(v['State']),str(v['County']),str(v['TotalPop']),str(v['Men']),str(v['Women']),str(v['Hispanic']),str(v['White']),str(v['Black']),str(v['Native']),str(v['Asian']),str(v['Pacific']),str(v['Citizen']),str(v['Income']),str(v['IncomeErr']),str(v['IncomePerCap']),str(v['IncomePerCapErr']),str(v['Poverty']),str(v['ChildPoverty']),str(v['Professional']),str(v['Service']),str(v['Office']),str(v['Construction']),str(v['Production']),str(v['Drive']),str(v['Carpool']),str(v['Transit']),str(v['Walk']),str(v['OtherTransp']),str(v['WorkAtHome']),str(v['MeanCommute']),str(v['Employed']),str(v['PrivateWork']),str(v['PublicWork']),str(v['SelfEmployed']),str(v['FamilyWork']),str(v['Unemployment']))
			csv_object.write('|'.join(tup)+'\n')
		cursor.copy_from(csv_object,TableName,sep='|')
		"""
		for cmd in icmdlist:
			# print (cmd)
			#cursor.execute(cmd)
                        psycopg2.extras.execute_batch(cursor,cmd,arglist)
		"""
		elapsed = time.perf_counter() - start
		print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')


def main():
    initialize()
    conn = dbconnect()
    rlis = readdata(Datafile)
    cmdlist = getSQLcmnds(rlis)

    if CreateDB:
    	createTable(conn)

    load(conn, cmdlist,rlis)


if __name__ == "__main__":
    main()



