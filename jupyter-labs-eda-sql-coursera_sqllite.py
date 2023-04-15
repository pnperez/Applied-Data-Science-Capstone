# -*- coding: utf-8 -*-
"""
Created on Sat Apr  8 17:46:40 2023

@author: pablo
"""

import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import sqlalchemy

# Define the server and database names
server = 'DESKTOP-D09OMTO' 
database = 'COURSERA' 
%load_ext sql
%sql mssql+pyodbc:///?server=DESKTOP-D09OMTO&database=COURSERA&driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes
# Create a SQLAlchemy engine object to connect to the SQL Server database
engine = create_engine(f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server')

# Load data
df = pd.read_csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DS0321EN-SkillsNetwork/labs/module_2/data/Spacex.csv")
df.head()

# load the data to sql
df.to_sql('SpaceX', con=engine, if_exists='replace', index=False,method="multi")

###### Task 1 ######
# Display the names of the unique launch sites in the space mission
%sql SELECT DISTINCT Launch_Site FROM SpaceX;

##### Task 2 #####
# Display 5 records where launch sites begin with the string 'CCA'
%sql SELECT TOP 5 * FROM SpaceX WHERE Launch_Site LIKE '%CCA%';

###### Task 3 ######
# Display the total payload mass carried by boosters launched by NASA (CRS)
%%sql
select sum(PAYLOAD_MASS__KG_) 
from SpaceX
where Customer = 'NASA (CRS)';

##### Task 4 ######
# Display average payload mass carried by booster version F9 v1.1
%%sql
SELECT AVG(PAYLOAD_MASS__KG_) AS AVG_PAYLOAD_MASS__KG_
FROM SpaceX
WHERE Booster_Version = 'F9 v1.1';

###### Task 5 #####
# List the date when the first succesful landing outcome in ground pad was acheived.
%%sql
SELECT MIN(Date)
FROM SpaceX
Where [Landing _Outcome] = 'Success (ground pad)';

####### Task 6 ######
# List the names of the boosters which have success in drone ship and have payload mass greater than 4000 but less than 6000
%%sql
SELECT DISTINCT(Customer)
FROM SpaceX
WHERE 
[Landing _Outcome] = 'Success (drone ship)' AND PAYLOAD_MASS__KG_ > 4000 AND PAYLOAD_MASS__KG_ < 6000;

###### Task 7 #####
# List the total number of successful and failure mission outcomes
%%sql
SELECT DISTINCT(Mission_Outcome), COUNT(Mission_Outcome) AS Count
FROM SpaceX
GROUP BY Mission_Outcome;

###### Task 8 #####
# List the names of the booster_versions which have carried the maximum payload mass. Use a subquery
%%sql
SELECT Booster_Version
FROM SpaceX
WHERE PAYLOAD_MASS__KG_ = (
  SELECT MAX(PAYLOAD_MASS__KG_)
  FROM SpaceX
);

##### Task 9 #####
# List the records which will display the month names, failure landing_outcomes in drone ship ,booster versions, launch_site for the months in year 2015.
%%sql
SELECT SUBSTRING(Date, 4, 2) AS Month,
       [Landing _Outcome],
       Booster_Version,
       Launch_Site
FROM SpaceX
WHERE SUBSTRING(Date, 7, 4) = '2015'
  AND [Landing _Outcome] LIKE 'Failure (%drone ship%)';
  
####### Task 10 ######
# Rank the count of successful landing_outcomes between the date 04-06-2010 and 20-03-2017 in descending order.
%%sql
SELECT [Landing _Outcome], COUNT(*) AS Success_count
FROM SpaceX
WHERE Date BETWEEN '04-06-2010' AND '20-03-2017'
  AND [Landing _Outcome] LIKE 'Success%'
GROUP BY [Landing _Outcome]
ORDER BY Success_count DESC;

%sql select min(PAYLOAD_MASS__KG_) from SpaceX

