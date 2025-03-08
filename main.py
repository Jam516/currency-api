from flask import Flask, request, jsonify
import requests
from datetime import datetime
import calendar
import os
import time
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Simple in-memory cache
cache = {}
CACHE_EXPIRY = 3600  # 1 hour

def validate_date(date_str):
    """Validate date string in YYYY-MM-DD format."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def get_months_between_dates(start_date, end_date):
    """Calculate all the months (YYYY-MM) between start_date and end_date."""
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    
    start_year, start_month = start_date_obj.year, start_date_obj.month
    end_year, end_month = end_date_obj.year, end_date_obj.month
    
    months = []
    current_year, current_month = start_year, start_month
    
    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        months.append(f"{current_year}-{current_month:02d}")
        
        if current_month == 12:
            current_month = 1
            current_year += 1
        else:
            current_month += 1
    
    return months

def get_month_range(year_month, start_date, end_date):
    """
    Get the start and end dates for a month.
    According to OpenExchangeRates docs, we can request the 1st to 31st of any month, 
    regardless of actual month length.
    """
    year, month = map(int, year_month.split('-'))
    
    # Always use the 1st day of the month as start
    month_start = f"{year}-{month:02d}-01"
    
    # Always use the 31st day for end (per API documentation)
    month_end = f"{year}-{month:02d}-31"
    
    # For the first month in the range, use the actual start date if it's after the 1st
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    if year == start_date_obj.year and month == start_date_obj.month:
        month_start = start_date
    
    # For the last month in the range, use the actual end date if it's before the 31st
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    if year == end_date_obj.year and month == end_date_obj.month:
        month_end = end_date
    
    return month_start, month_end

def transform_rates_format(rates_data):
    transformed_data = []
    
    # Iterate through each date
    for date, currencies in rates_data.items():
        # Iterate through each currency and its rate
        for currency, rate in currencies.items():
            # Create new object and add to result array
            transformed_data.append({
                "day": date,
                "price": rate,
                "symbol": currency
            })
    
    return transformed_data

@app.route('/api/exchange-rates', methods=['GET'])
def exchange_rates():
    try:
        # Get request parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        symbols = request.args.get('symbols')
        base = request.args.get('base')
        app_id = request.args.get('app_id') or os.environ.get('OPENEXCHANGERATES_APP_ID')
        
        # Validate inputs
        if not app_id:
            return jsonify({"error": "API key (app_id) is required"}), 400
            
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date are required"}), 400
            
        if not validate_date(start_date) or not validate_date(end_date):
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
            
        if datetime.strptime(start_date, '%Y-%m-%d') > datetime.strptime(end_date, '%Y-%m-%d'):
            return jsonify({"error": "start_date must be before end_date"}), 400
        
        # Check cache
        cache_key = f"{start_date}_{end_date}_{symbols}_{base}_{app_id}"
        if cache_key in cache and (time.time() - cache[cache_key]['timestamp']) < CACHE_EXPIRY:
            logger.info(f"Returning cached result for {start_date} to {end_date}")
            return jsonify(cache[cache_key]['data'])  # This is now just the rates
        
        # Calculate months between the dates
        months = get_months_between_dates(start_date, end_date)
        logger.info(f"Request spans {len(months)} months: {months}")
        
        # Initialize combined rates
        combined_rates = {}
        
        # Track metadata for internal use (not included in response)
        api_base = None
        
        # Loop through each month and make API calls
        for i, month in enumerate(months):
            month_start, month_end = get_month_range(month, start_date, end_date)
            
            # Prepare API call URL with correct parameter names
            url = f'https://openexchangerates.org/api/time-series.json?app_id={app_id}&start={month_start}&end={month_end}'
            
            # Add optional parameters if provided
            if symbols:
                url += f'&symbols={symbols}'
            
            if base:
                url += f'&base={base}'
            
            logger.info(f"Requesting data for {month_start} to {month_end}")
            # logger.info(f"API URL: {url}")
            
            # Add delay between requests (except for the first)
            if i > 0:
                time.sleep(1)  # 1 second delay to avoid rate limiting
            
            # Make API call
            response = requests.get(url)
            
            if response.status_code != 200:
                error_msg = f"API Error ({response.status_code}): {response.text}"
                logger.error(error_msg)
                return jsonify({"error": error_msg}), response.status_code
            
            # Parse response
            month_data = response.json()
            
            # Store base currency for internal reference
            if api_base is None:
                api_base = month_data.get("base", base or "USD")
            
            # Combine rate data
            combined_rates.update(month_data.get("rates", {}))
            
            logger.info(f"Added {len(month_data.get('rates', {}))} days of rates for {month}")
        
        # Transform data to flat array format
        transformed_data = transform_rates_format(combined_rates)
        
        # Cache the transformed result
        cache[cache_key] = {
            'data': transformed_data,
            'timestamp': time.time()
        }
        
        logger.info(f"Returning transformed data with {len(transformed_data)} entries")
        return jsonify(transformed_data)
    
    # Add detailed error logging for troubleshooting
    except Exception as e:
        error_msg = f"Error processing request: {str(e)}"
        logger.exception(error_msg)
        
        # Log additional diagnostic information
        if 'response' in locals() and hasattr(response, 'text'):
            logger.error(f"Response from OpenExchangeRates API: {response.text}")
        
        # Return a more informative error message
        return jsonify({
            "error": error_msg,
            "details": "Check server logs for more information"
        }), 500

@app.route('/', methods=['GET'])
def index():
    """API documentation endpoint."""
    return jsonify({
        "name": "Exchange Rates API",
        "description": "A wrapper for the OpenExchangeRates time-series API that handles date ranges spanning multiple months",
        "endpoint": "/api/exchange-rates",
        "parameters": {
            "start_date": "Start date (YYYY-MM-DD)",
            "end_date": "End date (YYYY-MM-DD)",
            "symbols": "Optional: Comma-separated currency codes (e.g., EUR,GBP,JPY)",
            "base": "Optional: Base currency (default: USD)",
            "app_id": "Your OpenExchangeRates API key"
        },
        "example": "/api/exchange-rates?start_date=2023-01-01&end_date=2023-03-31&symbols=EUR,GBP,JPY&app_id=YOUR_API_KEY",
        "response_format": "Returns data as an array of objects with day, price, and symbol properties",
        "notes": [
            "The API makes one request to OpenExchangeRates for each month in the date range",
            "Results are cached for 1 hour to improve performance",
            "The response includes a flat array of exchange rate entries"
        ]
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)