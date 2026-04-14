This is a Dashboard for sales aimed at small shops and entreprises. It provides a data breakdown of sales from a csv file.

You'll have to associate columns from your csv files to a set choices of columns in the dashboard in order for it to calculate and display data. 

Dashboard columns :
- Sales
- Quantity sold
- Year
- Month
- Product
- Country

The after initializing the csv files, an ETL pipeline will clean up missing data and proprely rename columns as previously.
You can then chose to filter data based on Year, Month and Country

The dashboard is as follows :

- Total sales (all years), average product price, total quantity sold
- Month sales, quantity sold this month, Difference in % compared to last year same month

A curve showing the evolution of sales throughout the year
A barchart of sales per product with the possibility of chosing the amount displayed (top 5, top 10, top 15)
A barchart of the most sold product regardless of its price with the same filter possibility

