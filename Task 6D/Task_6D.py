# %% [markdown]
# ### TASK 6D - pandas vs SQL
# ### Student name : Truong Khang Thinh Nguyen - 223446545
# ### Email : s223446545@gmail.com
# ### SIT220 - Undergraduate

# %% [markdown]
# ### In the realm of data manipulation and analysis, the parallels between SQL queries and Pandas data frame implementations are striking. Both tools are instrumental in extracting, transforming, and analyzing tabular data, with Pandas' data frames mirroring the concept of tables in SQL.
# ### This report embarks on an analysis to explore this convergence, focusing on harnessing the power of Pandas to implement tables and subsequently comparing the outcomes with those derived from SQL queries. By delving into these methodologies, we aim to unravel insights into their similarities, differences, and respective efficiencies in handling data operations.

# %% [markdown]
# #### In the initial phase of my analysis, it's imperative to lay the groundwork by importing essential packages that facilitate efficient manipulation of tabular data.
# #### Furthermore, I'll take the crucial step of creating and importing data files, setting the stage for subsequent data exploration and analysis. This foundational stage establishes the necessary infrastructure for my comparative study between Pandas data frame implementations and SQL queries.

# %%
# Import necessary packages 
import pandas as pd
import sqlite3

#Load csv files 
weather_df = pd.read_csv("weather.csv")
planes_df = pd.read_csv("planes.csv")
flights_df = pd.read_csv("flights.csv")
airlines_df = pd.read_csv("airlines.csv")
airports_df = pd.read_csv("airports.csv")

# Create / Connect to the SQLite Database
connection = sqlite3.connect("Task_6D.db")

# Load created dataframes into the database
weather_df.to_sql("weather",connection,if_exists = "replace",index = False)
planes_df.to_sql("planes",connection,if_exists = "replace",index = False)
flights_df.to_sql("flights",connection,if_exists = "replace",index = False)
airlines_df.to_sql("airlines",connection,if_exists = "replace",index = False)
airports_df.to_sql("airports",connection,if_exists = "replace", index = False)

# %% [markdown]
# #### In this code snippet, I imported Pandas library for tabular data analysis and SQLite for database management.
# #### Utilizing the read_csv() function, I loaded CSV files from my local machine into Pandas data frames, ensuring easy manipulation and exploration. Subsequently, using SQLite3, I established a database locally on my computer and created tables within it based on the previously created data frames using the to_sql() method.

# %% [markdown]
# #### For the initial phase of my analysis, I focused on extracting unique engine types utilized by each plane in the planes data set.

# %%
#SQL part
task1_sql =  pd.read_sql_query("""
    SELECT DISTINCT engine FROM planes
                                """,connection)
# Pandas only part
# Extract unique values from the engine column and then reset the index
task1_my = planes_df[["engine"]].drop_duplicates().reset_index(drop= True)
display(task1_my)

#Checking part
pd.testing.assert_frame_equal(task1_sql, task1_my) 

# %% [markdown]
# #### The provided code snippet creates a new DataFrame by extracting the unique engine types from the planes_df DataFrame. It achieves this by selecting the 'engine' column, removing duplicate entries using the drop_duplicates() method , and resetting the index.This Pandas operation efficiently captures the unique engine types utilized by each plane, mirroring the outcome of the SQL-based approach.

# %% [markdown]
# #### Expanding upon the initial task of returning unique engine types, I now enhanced my analysis to include the type of plane alongside the engine type. This will provide a more comprehensive view, allowing us to observe unique combinations of plane types and engine types within the dataset.

# %%
# SQL Part
task2_sql = pd.read_sql_query("""
    SELECT DISTINCT type, engine FROM planes
                            """,connection)
# Pandas only part
# Extract unique values from engine and type column and then reset the index
task2_my = planes_df[["type","engine"]].drop_duplicates().reset_index(drop = True)
display(task2_my)

# Checking
pd.testing.assert_frame_equal(task2_sql, task2_my) 

# %% [markdown]
# #### Just like the first code snippet. But this one will complement the 'type' column alongside the 'engine' column in the drop_duplicates() method.

# %% [markdown]
# #### For this part ,I calculated the number of airplanes utilizing distinct engine types.

# %%
# SQL part
task3_sql = pd.read_sql_query("""
SELECT COUNT(*), engine FROM planes GROUP BY engine
                            """,connection)

# Pandas only part
# Count the rows groupy engine column
task3_my = planes_df.groupby("engine")[["engine"]].count()

# Rename the engine column into the COUNT(*) to make it identical to the one in SQL query
task3_my.rename(columns = {"engine": "COUNT(*)"},inplace = True)
task3_my.reset_index(inplace = True)

# Sort columns of the 2 dataframes to make the order of columns identical
task3_my = task3_my.sort_index(axis = 1)
task3_sql = task3_sql.sort_index(axis = 1)
display(task3_my)

# Cheking 
pd.testing.assert_frame_equal(task3_sql, task3_my)

# %% [markdown]
# #### This task can be accomplished by employing the groupby() function, which groups the data by the engine column, followed by the count() function to calculate the number of rows corresponding to each type of engine. Additionally, for a successful comparison between the two data frames, it's essential to ensure that the columns are sorted in the same order.

# %% [markdown]
# #### Once again, this task involved determining the count of airplanes utilizing distinct types of planes along with the corresponding engine each plane employs.

# %%
# SQL part
task4_sql = pd.read_sql_query("""
SELECT COUNT(*), engine, type FROM planes
GROUP BY engine, type
                            """,connection)
# Pandas only part
# Group by engine and type column then use the size() function to count the rows of each unique value
task4_my = planes_df.groupby(["engine","type"]).size().to_frame().reset_index()

# Rename the column
task4_my.rename(columns = {0:"COUNT(*)"},inplace = True)

# Sort columns
task4_my = task4_my.sort_index(axis = 1)
task4_sql = task4_sql.sort_index(axis = 1)

display(task4_my)

# Checking 
pd.testing.assert_frame_equal(task4_sql, task4_my)

# %% [markdown]
# #### This task can be achieved by grouping the DataFrame using the groupby function on both the 'engine' and 'type' columns, followed by applying the size() function to count the number of rows for each unique combination of engine and plane type. Since size() returns a Series, I converted it into a DataFrame using the to_frame() function. Ensuring the order of columns in both DataFrames is sorted will facilitate successful comparison between them.

# %% [markdown]
# #### For the next phase of the analysis, I have created a table that showcases the maximum and minimum production years for a specific engine type, alongside the average production year, with the respective manufacturer responsible for producing that engine.

# %%
# SQL part
task5_sql = pd.read_sql_query("""
SELECT MIN(year), AVG(year), MAX(year), engine, manufacturer
FROM planes
GROUP BY engine, manufacturer
                            """,connection)

# Pandas only part
task5_my = planes_df.groupby(["engine","manufacturer"]).aggregate(
                        {"year":["min","mean","max"]}).reset_index()
# Rename columns
task5_my.columns = ["engine","manufacturer","MIN(year)","AVG(year)","MAX(year)"]

# Re-arrange the columns order
task5_my = task5_my.reindex(columns = ["MIN(year)","AVG(year)","MAX(year)","engine","manufacturer"])
display(task5_my.head())

# Checking 
pd.testing.assert_frame_equal(task5_sql, task5_my)

# %% [markdown]
# #### In the code snippet, I utilized the groupby function to group the data by both the engine and the manufacturer responsible for that engine. Then, I employed the aggregate function to perform multiple calculations (min, mean, and max) on the 'year' column. Since the resulting DataFrame from Pandas doesn't match the structure of the one from the SQL query, I proceeded to rename the columns and rearrange them to align with the SQL-generated DataFrame.

# %% [markdown]
# #### In the subsequent step, I identified all the planes that have a non-null speed value, meaning that the speed information was not available and recorded during the data gathering process.

# %%
# SQL Part
task6_sql = pd.read_sql_query("""
SELECT * FROM planes WHERE speed IS NOT NULL
                            """,connection)

# Pandas only part
task6_my = planes_df[planes_df["speed"].notna()].reset_index(drop = True)
display(task6_my.head())

# Checking 
pd.testing.assert_frame_equal(task6_sql, task6_my)

# %% [markdown]
# #### The code snippet accomplishes this task by utilizing the notna() function, which is applied to the 'speed' column. This function filters out the rows where the speed values are not null, effectively identifying all the planes with recorded speed information.

# %% [markdown]
# #### In this section, I identified the tail numbers of each plane by filtering the dataset to include only those planes with a seating capacity falling within the range of 150 to 210 seats and a production year later than 2011.

# %%
# SQL Part
task7_sql = pd.read_sql_query("""
SELECT tailnum FROM planes
WHERE seats BETWEEN 150 AND 210 AND year >= 2011
                            """,connection)

# Pandas only part
filtered_df = planes_df[ ( (planes_df["seats"] >= 150) & (planes_df["seats"] <= 210) )
                         & (planes_df["year"] >= 2011)] 

# Filter out other columns which only has the tailnum column only
task7_my = filtered_df[["tailnum"]].reset_index(drop = True)
display(task7_my.head())

# Checking
pd.testing.assert_frame_equal(task7_sql, task7_my) 

# %% [markdown]
# #### This task can be achieved by directly implementing a mask operation ( seats >= 150 and seats <= 210 and year > 2011 ) while filtering out the rows. Subsequently, from the created tables, I restricted the columns to just one column, which contains the tail numbers.

# %% [markdown]
# #### In the subsequent step, I returned the planes, specifically their tail numbers, along with the corresponding manufacturer. This was done under the condition that the seat number exceeds 390 and the manufacturer is either BOEING, AIRBUS, or EMBRAER.

# %%
# SQL Part
task8_sql = pd.read_sql_query("""
SELECT tailnum, manufacturer, seats FROM planes
WHERE manufacturer IN ("BOEING", "AIRBUS", "EMBRAER") AND seats>390
                            """,connection)

# Pandas only 
task8_my = planes_df.query('(manufacturer == "BOEING" or '
                           'manufacturer == "AIRBUS" or '
                           'manufacturer == "EMBRAER") '
                           'and seats > 390')
task8_my = task8_my[["tailnum","manufacturer","seats"]].reset_index(drop = True)
display(task8_my)

# Checking
pd.testing.assert_frame_equal(task8_sql, task8_my) 

# %% [markdown]
# #### In the provided code snippet, I utilized the query function to filter out rows based on conditions specified for both the manufacturer and the number of seats. The logical OR operation (|) was employed for the manufacturer column, allowing for filtering of rows where the manufacturer is either BOEING, AIRBUS, or EMBRAER. Additionally, the logical AND operation (&) was used to ensure that the seat number exceeds 390. Finally, I restricted the resulting DataFrame to contain only three columns: tail number, manufacturer, and seats number.

# %% [markdown]
# #### In this section, I constructed a table comprising the unique production years and seating capacities for specific planes. The condition set was that the production year had to be equal to or greater than 2012. Additionally, the table was arranged in ascending order for the year column and descending order for the seats column.

# %%
# SQL Part 
task9_sql = pd.read_sql_query("""
SELECT DISTINCT year, seats FROM planes
WHERE year >= 2012 ORDER BY year ASC, seats DESC
                            """,connection)

# Pandas only part
# Filter out the year 
filtered_year = planes_df.query('year >= 2012')

# Return unique values from the year and the seats column
# As well as sorting the orders of those 2 columns
task9_my = filtered_year[["year","seats"]].drop_duplicates().sort_values(
                        by = ["year","seats"], ascending = [True,False]).reset_index(
                        drop = True)
display(task9_my.head())

# Checking
pd.testing.assert_frame_equal(task9_sql, task9_my) 

# %% [markdown]
# #### To achieve this, I employed the query() function to filter out the rows where the production year is equal to or greater than 2012. Then, I used the drop_duplicates() method to retain only the unique combinations of production year and seating capacity. Finally, I sorted the values in ascending order for the year column and descending order for the seats column using the sort_values() function.

# %% [markdown]
# #### In this section, the approach is opposite to the previous one regarding the order of the year and seat columns. Specifically, the table is arranged in ascending order for the seating capacity and descending order for the production year.

# %%
# SQL Part 
task10_sql = pd.read_sql_query("""
SELECT DISTINCT year, seats FROM planes
WHERE year >= 2012 ORDER BY seats DESC, year ASC
                            """,connection)

# Pandas only
filter_df_year = planes_df.query('year >= 2012')
task10_my = filter_df_year[["year","seats"]].drop_duplicates().sort_values(
                            by = ["seats","year"],ascending = [False,True]).reset_index(
                            drop = True)
display(task10_my.head())

# Checking
pd.testing.assert_frame_equal(task10_sql, task10_my)

# %% [markdown]
# #### The code for this section is almost identical to the previous one, except for reversing the order of sorting for the year and seating columns. By setting the ascending parameter to True for the year column and False for the seating column, we achieve the desired arrangement.

# %% [markdown]
# #### In the subsequent part, I generated a table that depicts the number of planes manufactured by different manufacturers, specifically focusing on planes with a seating capacity exceeding 200 seats.

# %%
# SQL Part 
task11_sql = pd.read_sql_query("""
SELECT manufacturer, COUNT(*) FROM planes
WHERE seats > 200 GROUP BY manufacturer
                            """,connection)

# Pandas only part
# Filter out the rows 
filtered_seat = planes_df.query('seats > 200')

# Return the numbe of rows by different manufacturers
task11_my = filtered_seat.groupby("manufacturer").size().to_frame().reset_index()
task11_my = task11_my.rename(columns = {0:"COUNT(*)"})
display(task11_my)

#Checking
pd.testing.assert_frame_equal(task11_sql, task11_my)

# %% [markdown]
# #### In the provided code snippet, the query function was utilized to filter out the rows where the seating capacity is greater than 200. Subsequently, the size() function was applied to count the number of rows for each unique manufacturer, effectively determining the number of planes manufactured by different manufacturers with a seating capacity exceeding 200 seats.

# %% [markdown]
# #### For the next part, I returned a table that performs the same function as the previous section, with the added condition that the planes returned by different manufacturers must exceed 10 planes in total.

# %%
# SQL Part 
task12_sql = pd.read_sql_query("""
SELECT manufacturer, COUNT(*) FROM planes
GROUP BY manufacturer HAVING COUNT(*) > 10
                            """,connection)

# Pandas only
# Group by different manufacturers and count number of planes
task12_my = planes_df.groupby("manufacturer").size().to_frame()

# Filter out the rows as well as renaming the column
task12_my = task12_my[task12_my[0] > 10].reset_index()
task12_my.rename(columns = {0:"COUNT(*)"},inplace = True)
display(task12_my)

# Checking
pd.testing.assert_frame_equal(task12_sql, task12_my)

# %% [markdown]
# #### To achieve this, the code snippet is the same to the previous one with a slight modification in the filtering step. Instead of filtering out planes with seating capacity exceeding 200 seats, a mask operation is applied to filter out manufacturers with more than 10 planes.

# %% [markdown]
# #### For the next part, interestingly , the task will combine the conditions from the two previous steps to filter the rows. Specifically, it will filter the rows based on two criteria: the seating capacity of the plane must exceed 200 seats, and the number of planes manufactured by each manufacturer must be greater than 10.
#
# %%
# SQL Part 
task13_sql = pd.read_sql_query("""
SELECT manufacturer, COUNT(*) FROM planes
WHERE seats > 200 GROUP BY manufacturer HAVING COUNT(*) > 10
                             """,connection)

# Pandas only part
# Filter out the rows which have the seats > 200
filtered_seat = planes_df.query('seats > 200')

# Group by different manufacturers
task13_my = filtered_seat.groupby("manufacturer").size().to_frame()

# Rename the column name as well as filter out the rows one more time
task13_my.rename(columns = {0:"COUNT(*)"},inplace = True)
task13_my = task13_my[task13_my["COUNT(*)"] > 10].reset_index()
display(task13_my)

# Checking 
pd.testing.assert_frame_equal(task13_sql, task13_my)

# %% [markdown]
# #### From the provided code snippet, the task can be achieved by simply combining the two previous approaches. This entails filtering rows based on both conditions: seating capacity exceeding 200 seats using query() function and the number of planes manufactured by each manufacturer exceeding 10 using mask operation.

# %% [markdown]
# #### For the next part, I identified the top 10 manufacturers with the highest number of planes manufactured. This was achieved by counting the total number of planes manufactured by each manufacturer and selecting the top 10 manufacturers based on this count.

# %%
# SQL Part 
task14_sql = pd.read_sql_query("""
SELECT manufacturer, COUNT(*) AS howmany
FROM planes
GROUP BY manufacturer
ORDER BY howmany DESC LIMIT 10
                            """,connection)
# Pandas only
# Group by different manufacturers

task14_my = planes_df.groupby("manufacturer").size().to_frame()

# Sort the returned number of planes in descending order
task14_my.sort_values(by = 0,ascending = False,inplace = True)

# Rename the column 
task14_my = task14_my.rename(columns = {0:"howmany"}).reset_index()

# Extract the top 10 number of planes returned
task14_my = task14_my.iloc[:10]
display(task14_my)

# Checking 
pd.testing.assert_frame_equal(task14_sql, task14_my)

# %% [markdown]
# #### In terms of code implementation, the process involved grouping the manufacturers using the groupby() function, followed by applying the size() function to count the number of planes for each manufacturer. The resulting count of planes was then sorted in descending order using the sort_values() function, with the ascending parameter set to False, making the column more readable by assigning a descriptive name. Finally, the top 10 manufacturers were extracted using the iloc() method.

# %% [markdown]
# #### For the next part, it selects all columns from the 'flights' table and adds additional columns from the 'planes' table. The tables are joined using a LEFT JOIN operation based on the 'tailnum' column, which represents the tail number of the planes. The purpose of this task is to combine data from both the 'flights' and 'planes' tables, providing detailed information about each flight, including the year of the plane, its speed, and seating capacity. This allows for comprehensive analysis of flight data alongside corresponding plane details.

# %%
# SQL Part 
task15_sql = pd.read_sql_query("""
SELECT
flights.*,
planes.year AS plane_year,
planes.speed AS plane_speed,
planes.seats AS plane_seats
FROM flights LEFT JOIN planes ON flights.tailnum=planes.tailnum
                            """,connection)

# Pandas only part
# Filter out the columns
extract_planes_df = planes_df[["year","speed","seats","tailnum"]]

# Left Join the 2 dataframes
task15_my =pd.merge(flights_df,extract_planes_df,
                          how = "left",
                           on = "tailnum")

# Rename the columns name of the newly created dataframe
task15_my.rename(columns = {"year_x":"year","year_y":"plane_year","speed":"plane_speed",
                           "seats":"plane_seats"},inplace = True)
display(task15_my.head())

# Checking
pd.testing.assert_frame_equal(task15_sql, task15_my)

# %% [markdown]
# #### This code snippet creates a new DataFrame by merging data from two existing DataFramess,specifically using the Left Join operation, flights_df and extract_planes_df, based on a common column 'tailnum', which represents the plane's unique identifier. The merged DataFrame includes flight information alongside corresponding details about each plane, such as its production year ('plane_year'), speed ('plane_speed'), and seating capacity ('plane_seats'). The columns are renamed for clarity, facilitating further analysis and interpretation of flight data alongside associated plane attributes.

# %% [markdown]
# #### For the next part, it retrieves data from the 'planes' and 'airlines' tables, filtering them based on flight data. It starts by selecting distinct combinations of carrier and tail number from the 'flights' table. Then, it joins this filtered data with the 'planes' table using the tail number as the common identifier. Additionally, it joins the 'airlines' table using the carrier code from the flight data, enabling the extraction of detailed information about planes and the associated airlines from the flight records.

# %%
# SQL Part 
task16_sql = pd.read_sql_query("""
SELECT planes.*, airlines.* FROM
(SELECT DISTINCT carrier, tailnum FROM flights) AS cartail
INNER JOIN planes ON cartail.tailnum=planes.tailnum
INNER JOIN airlines ON cartail.carrier=airlines.carrier
                            """,connection)
# Pandas only part
cartail_df = flights_df[["carrier","tailnum"]].drop_duplicates()
task16_my = planes_df.merge(cartail_df, on="tailnum", how="inner").merge(airlines_df
                                                , on="carrier", how="inner")

# Sort the values of the specific columns to easily check the similarity of the 2 dataframes
task16_my = task16_my.sort_values(by = ["tailnum","year","carrier"]).reset_index(drop = True)
task16_sql= task16_sql.sort_values(by = ["tailnum","year","carrier"]).reset_index(drop = True)
display(task16_my)

# Checking 
pd.testing.assert_frame_equal(task16_sql, task16_my)

# %% [markdown]
# #### This code snippet merges data from three DataFrames: flights_df, planes_df, and airlines_df. It begins by creating cartail_df to select distinct combinations of carrier and tail number from flight data. Then, it merges planes_df with cartail_df based on the tail number and subsequently merges the result with airlines_df based on the carrier. Finally, the merged DataFrame is sorted by tail number, year, and carrier for better organization as well checking the SQL created datframe and the Pandas created dataframe.

# %% [markdown]
# #### The final task serves to combine flight data from (EWR) airport with corresponding weather information, specifically average temperature (atemp) and average humidity (ahumid), for each flight day. It achieves this by initially selecting flight records from the 'flights' table where the origin is EWR. These selected flights are then matched with weather data from the 'weather' table, which is also filtered to include only records from EWR.
# #### The join operation is based on the year, month, and day columns, aligning flight dates with weather data for comprehensive analysis. By integrating these datasets, the query aims to provide insights into potential correlations between flight patterns and weather conditions at EWR Airport.

# %%
# SQL Part 
task17_sql = pd.read_sql_query("""
SELECT
flights2.*,
atemp,
ahumid
FROM (
SELECT * FROM flights WHERE origin='EWR'
) AS flights2
LEFT JOIN (
SELECT
year, month, day,
AVG(temp) AS atemp,
AVG(humid) AS ahumid
FROM weather
WHERE origin='EWR'
GROUP BY year, month, day
) AS weather2
ON flights2.year=weather2.year
AND flights2.month=weather2.month
AND flights2.day=weather2.day
                            """,connection)

# Pandas only part
# First Dataframe
flights2 = flights_df.query('origin == "EWR"')

# Second Dataframe
weather2 = weather_df.query('origin == "EWR"')
weather2 = weather2.groupby(["year","month","day"]).aggregate({"temp":"mean",
                                            "humid":"mean"}).reset_index()
weather2.rename(columns = {"temp":"atemp","humid":"ahumid"},inplace = True)


# Left-joining the 2 dataframes
task17_my = flights2.merge(weather2, on = ["year","month","day"],
                          how = "left")
display(task17_my.head())

# Checking 
pd.testing.assert_frame_equal(task17_sql, task17_my) 

# %% [markdown]
# #### This code snippet first filters the weather_df DataFrame to include only records where the origin is "EWR", representing the EWR Airport. Then, it groups the filtered DataFrame by year, month, and day, aggregating the temperature and humidity values to calculate their means for each day.
# #### These aggregated values are then renamed to "atemp" (average temperature) and "ahumid" (average humidity) for clarity. Finally, the flights2 DataFrame is left-joined with the weather2 DataFrame based on the year, month, and day columns, incorporating weather data into flight records for further analysis.

# %%
# Commit changes and close the database
connection.commit()
connection.close()

# %% [markdown]
# ### In conclusion, this report underscores the effectiveness of utilizing Pandas methods and functions for database manipulation and analysis, in comparison to traditional SQL queries. By harnessing Pandas' robust functionalities, such as data frame operations, grouping, filtering, and merging, we have demonstrated the capability to efficiently handle and analyze tabular data within a database environment.
# ### When considering which tool to use for data manipulation, the choice between Pandas and SQL ultimately depends on various factors, including the nature of the task, the complexity of the data, and the user's familiarity with each tool.
# ### Pandas excels in scenarios where data manipulation involves extensive exploratory analysis, complex transformations, and integration with other Python libraries for data visualization and machine learning. Its intuitive syntax, vast array of functions, and seamless integration with the Python ecosystem make it an ideal choice for data scientists and analysts working with structured tabular data.
# ### On the other hand, SQL shines in scenarios where data manipulation primarily involves querying and aggregating data stored in relational databases. It offers optimized performance for handling large datasets and complex joins, making it well-suited for tasks such as data retrieval, aggregation, and filtering directly from a database management system.
