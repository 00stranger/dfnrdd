Covid-19 Data Analysis
Section 1 — Data Cleaning, Aggregation & Sorting

Rename the column infection_case to infection_source
Select only the following columns: Province, city, infection_source, confirmed
Change the datatype of the confirmed column to integer
Return TotalConfirmed and MaxFromOneConfirmedCase for each (Province, city) pair
Sort the output in ascending order by TotalConfirmed


Section 2 — Top Provinces

Return the top 2 provinces based on confirmed cases


Section 3 — Filtered View for Daegu

Filter rows where Province == 'Daegu' and confirmed > 10
Drop the following columns and select all others:
