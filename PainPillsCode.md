 scp /Users/anushavalasapalli/Desktop/arcos_all_washpost.tsv avalasa@52.12.203.116:~/

 ssh avalasa@52.12.203.116

 hdfs dfs -mkdir /user/avalasa/PainPillsInTheUSA
 hdfs dfs -ls /user/avalasa/PainPillsInTheUSA

 hdfs dfs -mkdir /user/avalasa/PainPillsInTheUSA/PainPillsInTheState
 hdfs dfs -ls /user/avalasa/PainPillsInTheUSA/PainPillsInTheState

 hdfs dfs -mkdir /user/avalasa/PainPillsInTheUSA/PainPillsInTheCity
 hdfs dfs -ls /user/avalasa/PainPillsInTheUSA/PainPillsInTheCity

 hdfs dfs -mkdir /user/avalasa/PainPillsInTheUSA/MostPopularPainPills
 hdfs dfs -ls /user/avalasa/PainPillsInTheUSA/MostPopularPainPills

 hdfs dfs -mkdir /user/avalasa/PainPillsInTheUSA/MostPopularPainPillsInParticularStates
 hdfs dfs -ls /user/avalasa/PainPillsInTheUSA/MostPopularPainPillsInParticularStates


 hdfs dfs -put arcos_all_washpost.tsv /user/avalasa/PainPillsInTheUSA/

 hdfs dfs -cat /user/avalasa/PainPillsInTheUSA/arcos_all_washpost.tsv
 hdfs dfs -ls /user/avalasa/PainPillsInTheUSA/

 hdfs dfs -cat /user/avalasa/PainPillsInTheUSA/arcos_all_washpost.tsv | head -n 5



USE avalasa;

CREATE EXTERNAL TABLE if not exists Pain_Pills_In_The_USA(
REPORTER_DEA_NO string,
REPORTER_BUS_ACT string,
REPORTER_NAME string,
REPORTER_ADDL_CO_INFO string,
REPORTER_ADDRESS1 string,
REPORTER_ADDRESS2 string,
REPORTER_CITY string,
REPORTER_STATE string,
REPORTER_ZIP string,
REPORTER_COUNTY string,
BUYER_DEA_NO string,
BUYER_BUS_ACT string,
BUYER_NAME string,
BUYER_ADDL_CO_INFO string,
BUYER_ADDRESS1 string,
BUYER_ADDRESS2 string,
BUYER_CITY string,
BUYER_STATE string,
BUYER_ZIP string,
BUYER_COUNTY string,
TRANSACTION_CODE string,
DRUG_CODE string,
NDC_NO string,
DRUG_NAME string,
QUANTITY DOUBLE,
UNIT string,
ACTION_INDICATOR string,
ORDER_FORM_NO string,
CORRECTION_NO string,
STRENGTH string,
TRANSACTION_DATE string,
CALC_BASE_WT_IN_GM DOUBLE,
DOSAGE_UNIT DOUBLE,
TRANSACTION_ID DOUBLE,
Product_Name string,
Ingredient_Name string,
Measure string,
MME_Conversion_Factor INT,
Combined_Labeler_Name string,
Revised_Company_Name string,
Reporter_family string,
dos_str DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION '/user/avalasa/PainPillsInTheUSA/'
TBLPROPERTIES ('skip.header.line.count'='1');


select * from Pain_Pills_In_The_USA limit 10;
select count(*) from Pain_Pills_In_The_USA;

Count 178,598,026


State Pain Pills Count:-


DROP TABLE IF EXISTS Pain_Pills_In_The_USA_Per_State;
CREATE TABLE Pain_Pills_In_The_USA_Per_State
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/avalasa/PainPillsInTheUSA/PainPillsInTheState'
AS
select BUYER_STATE, sum(QUANTITY) from Pain_Pills_In_The_USA group by BUYER_STATE order by 2 desc;



select count(*) from Pain_Pills_In_The_USA_Per_State;
select * from Pain_Pills_In_The_USA_Per_State limit 10;



hdfs dfs -ls /user/avalasa/PainPillsInTheUSA/PainPillsInTheState

hdfs dfs -cat /user/avalasa/PainPillsInTheUSA/PainPillsInTheState/000000_0 | tail -n 2





hdfs dfs -get /user/avalasa/PainPillsInTheUSA/PainPillsInTheState/000000_0 PainPills_In_The_USA_States.txt


readlink -f PainPills_In_The_USA_States.txt // to know linux system path

scp avalasa@52.12.203.116:/home/avalasa/PainPills_In_The_USA_States.txt .




City Pain Pills Count:-

BUYER_CITY

DROP TABLE IF EXISTS Pain_Pills_In_The_USA_Per_Cities;
CREATE TABLE Pain_Pills_In_The_USA_Per_Cities
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/avalasa/PainPillsInTheUSA/PainPillsInTheCity'
AS
select BUYER_CITY, sum(QUANTITY) from Pain_Pills_In_The_USA group by BUYER_CITY order by 2 desc;

select count(*) from Pain_Pills_In_The_USA_Per_Cities;
select * from Pain_Pills_In_The_USA_Per_Cities limit 10;

hdfs dfs -ls /user/avalasa/PainPillsInTheUSA/PainPillsInTheCity

hdfs dfs -cat /user/avalasa/PainPillsInTheUSA/PainPillsInTheCity/000000_0 | tail -n 2

hdfs dfs -get /user/avalasa/PainPillsInTheUSA/PainPillsInTheCity/000000_0 PainPills_In_The_USA_Cities.txt


readlink -f PainPills_In_The_USA_Cities.txt // to know linux system path

scp avalasa@52.12.203.116:/home/avalasa/PainPills_In_The_USA_Cities.txt .



MostPopularPainPills


DROP TABLE IF EXISTS Most_Popular_Pain_Pills_In_The_USA;
CREATE TABLE Most_Popular_Pain_Pills_In_The_USA
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/avalasa/PainPillsInTheUSA/MostPopularPainPills'
AS
select DRUG_NAME, sum(QUANTITY) from Pain_Pills_In_The_USA where DRUG_NAME is not null group by DRUG_NAME order by 2 desc;

select count(*) from Most_Popular_Pain_Pills_In_The_USA;
select * from Most_Popular_Pain_Pills_In_The_USA limit 10;


hdfs dfs -ls /user/avalasa/PainPillsInTheUSA/MostPopularPainPills

hdfs dfs -cat /user/avalasa/PainPillsInTheUSA/MostPopularPainPills/000000_0 | tail -n 2

hdfs dfs -get /user/avalasa/PainPillsInTheUSA/MostPopularPainPills/000000_0 PainPills_In_The_USA_Particular_Cities.txt


readlink -f PainPills_In_The_USA_Particular_Cities.txt // to know linux system path

scp avalasa@52.12.203.116:/home/avalasa/PainPills_In_The_USA_Particular_Cities.txt .



Most Popular Pain Pills in FL, TX and CA

DROP TABLE IF EXISTS Popular_Pain_Pills_In_FL_TX_CA;
CREATE TABLE Popular_Pain_Pills_In_FL_TX_CA
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/avalasa/PainPillsInTheUSA/MostPopularPainPillsInParticularStates'
AS
select DRUG_NAME, sum(QUANTITY) from Pain_Pills_In_The_USA where DRUG_NAME is not null and  (lower(BUYER_STATE) like "ca" or lower(BUYER_STATE) like "fl" or lower(BUYER_STATE) like "tx") group by DRUG_NAME order by 2 desc;

select count(*) from Popular_Pain_Pills_In_FL_TX_CA;
select * from Popular_Pain_Pills_In_FL_TX_CA limit 10;


hdfs dfs -ls /user/avalasa/PainPillsInTheUSA/MostPopularPainPillsInParticularStates

hdfs dfs -cat /user/avalasa/PainPillsInTheUSA/MostPopularPainPillsInParticularStates/000000_0 | tail -n 2

hdfs dfs -get /user/avalasa/PainPillsInTheUSA/MostPopularPainPillsInParticularStates/000000_0 PainPills_In_The_Particular_States_FL_TX_CA.txt


readlink -f PainPills_In_The_Particular_States_FL_TX_CA.txt // to know linux system path

scp avalasa@52.12.203.116:/home/avalasa/PainPills_In_The_Particular_States_FL_TX_CA.txt 
