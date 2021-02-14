# DWBI Basics
1. **What?** Need to create a solution which can be applied across all databases used in our department and integrated to our CI/CD pipelines. For analysis purposes need to have a table called DATE_DIM. It should contain at least 15 columns. Date range: 1/1/2020 - 12/31/2020. Grain: 1 day. Database - your choice. 
1. **Technical details:** Solution can be split into 2 parts.
   1. part - SQL scripts designed for creation of the table, should be implemented as DROP/CREATE. Feel free to use DDL statements or Stored Procedures. This SQL script should be called into database via executor - could be .sh/.bat scripts, other languages such as Python/Java/etc. 
   1. part - ETL scripts for data population.  
   
### Technical description   
1. Choose database - MS-SQL server used in previous labs can be re-used. Or feel free to use other DB using education license as well! 
1. Create .sql script OR stored procedure to deploy table DATE_DIM. It should contain at least 10 columns, can be expanded to 15 if needed. Please use info from provided materials/online resources regarding table structure. 
1. Create a solution to populate this table - it should be a one-time program which should be restartable. Please see ideas how it can be done: 
   1. Data generator using Python 
   1. API calls to download file (.csv/.json/other) 
   1. .sh or .bat scripts 
   1. Other... 
1. Date range - 1/1/2020 till 12/31/2020 
1. Final result - folder in GIT, your account, lowercase. It should contain: Program, 1 min or less video/gif with execution steps, Documentation as README file (description, screenshots, explanation, demo, etc) 
