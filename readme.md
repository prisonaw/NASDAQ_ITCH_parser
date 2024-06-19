# Running VWAP calculation with ITCH 5.0 data


### Output

`/out` folder has VWAP results for each trading hour, and the files are named by the hour. The files contain the ticker symbol along the the running volume weighted average price for that hour.

### Prerequisites

Python 3.5

### Documentation used 
http://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHspecification.pdf


### Steps to execute
1. Add the following file to the `/data` folder: https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/01302019.NASDAQ_ITCH50.gz

2. Navigate to the `/src` folder run the following comment in your terminal :  python3 solution.py
