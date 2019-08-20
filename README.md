# goszakup
A simple project to build database of protocols from goszakup.gov.kz and to analyze them to predict winning prices.
This script is customizable to pull data from goszakup.gov.kz.
The script with writen in py3, script file is main.py.
Additionals .txt files are used for debugging, to check if script successfully pulls the data.
Data is stored in mongoDB database, therefore script needs mongodb install in the computer to run.
MongoDB is created in localhost.

As of version 0.1, script is functional to scrape data and to submit to mongodb, database is structured.
to do:
  
 *make script to be able to parse .pdf files (currently supports only html)

 *optimize code to be customizable, rewrite hardcoded parts.
 
 *reduce execution time, need to review for loops.
 
 *build better database structure
 
 *implement simple linear regression model to predict winning prices

