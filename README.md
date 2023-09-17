
## Local Dev
 start storage emulator : 
 > azurite
 > azurite --silent --location ~/dev/atom-analysis --tableHost 127.0.0.1 --tablePort 10002

C:\Users\aftab.jalal\dev\AzuriteData


# atom-analysis: Population-level trends for Client Outcome Measures

## Changes in average (over clients) scores ATOM Assessment Survey questions.

(see b.ipynb file)

## Steps:

1. Extract the data - from the database or from a pre-prepared parquet file
2. Processing: if not pulling from pre-cleaned data: Clean & Transform (incl. categorize) the data
    - Clean data - remove rows with missing data : PDCSubstanceOrGambling
    - Transform data - expand PDC, determine Program, categorize fields, drop Notes/Comments fields, rename PartitionKey to SLK.
    - Limit the data to the period of interest - i.e. only clients who have completed at least one survey during the period of interest.
    - Limit by only clients who have completed the survey at least three times (min-stage: 3)
5. Calculate the average score for each client for each stage and for each of the questions of interest.




### Step 1 & 2: Extract & Process

#### Extract the data - from the database or from a pre-prepared parquet file

1. *Processed data*:
  - if processed-parquet file is not present, *get the raw data* and process it and cache it into the parquet file.
  - if yes, load the data from the parquet file.
  
2. If *Raw data* doesn't exist in the data/in/ folder as a parquet file:
  - load it from the database (Azure)
  - otherwise from the parquet file.
 
 (cache=True => try to load from a parquet file, if not present, load from the database and cache it into a parquet file)



 ### Step 3 : Calculate the average score for each client for each stage and for each of the questions of interest.



 ## Episode Data

 ### Get Episode Data from Communicare Database in to Azure Table Storage
  *Instructions*:
   https://directionshealth.sharepoint.com/:w:/g/EQSqD2cJZK5JqN1k7x-BHwABEeiJxYYpeNX8ZOP9VIH3Jw?e=xABkCN

 ### Run match_audit.py (to be hosted as a streamlit web app)


  
