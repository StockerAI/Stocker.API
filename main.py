from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import select, and_, text
from datetime import datetime
from config import config
from sqlalchemy.exc import SQLAlchemyError

app = Flask(__name__)

# Database configuration
db_config = config()
db_url = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
app.config['SQLALCHEMY_DATABASE_URI'] = db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

def reflect_db():
    with app.app_context():
        db.Model.metadata.reflect(db.engine)

# Call this function after the app has started
reflect_db()

@app.route('/get_tickers', methods=['GET'])
def get_tickers():
    try:
        tickers_table = db.Model.metadata.tables['Tickers']
        ticker_list = request.args.getlist('ticker_name')

        columns_to_select = [tickers_table.c.tickerName, tickers_table.c.stockMarket]

        if ticker_list:
            select_statement = select(*columns_to_select).where(tickers_table.c.tickerName.in_(ticker_list))
        else:
            select_statement = select(*columns_to_select)

        try:
            with db.engine.connect() as connection:
                result = connection.execute(select_statement)
                rows = result.fetchall()

                # Convert each Row object to a dictionary using _asdict()
                tickers_list = [row._asdict() for row in rows]
                return jsonify(tickers_list)
        except SQLAlchemyError as e:
            print("SQLAlchemy Error:", str(e))
            return jsonify({"error": str(e)}), 500
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get_stocks', methods=['GET'])
def get_stocks():
    stocks_table = db.Model.metadata.tables['Stocks']
    tickers_table = db.Model.metadata.tables['Tickers']

    # Get query parameters
    ticker_list = request.args.getlist('ticker_name')
    stock_market_list = request.args.getlist('stock_market_name')
    starting_date = request.args.get('starting_date')
    ending_date = request.args.get('ending_date')

    # Define the columns to select
    columns_to_select = [
        tickers_table.c.tickerName, 
        stocks_table.c.open, 
        stocks_table.c.close, 
        stocks_table.c.high, 
        stocks_table.c.low, 
        stocks_table.c.adjclose, 
        stocks_table.c.volume, 
        stocks_table.c.date
    ]

    # Construct the SELECT statement with necessary joins and filters
    query = select(*columns_to_select).select_from(
        stocks_table.join(tickers_table, stocks_table.c.tickerId == tickers_table.c.tickerId)
    )

    # Apply filters
    if ticker_list and stock_market_list:
        query = query.where(and_(tickers_table.c.tickerName.in_(ticker_list), 
                                tickers_table.c.stockMarket.in_(stock_market_list)))

    if starting_date:
        starting_date_obj = datetime.strptime(starting_date, '%Y-%m-%d')
        query = query.where(stocks_table.c.date >= starting_date_obj)
    if ending_date:
        ending_date_obj = datetime.strptime(ending_date, '%Y-%m-%d')
        query = query.where(stocks_table.c.date <= ending_date_obj)

    # Order the results
    query = query.order_by(tickers_table.c.tickerId, stocks_table.c.date)

    try:
        with db.engine.connect() as connection:
            result = connection.execute(query)
            stocks_list = [row._asdict() for row in result]
        return jsonify(stocks_list)
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get_company_details', methods=['GET'])
def get_company_details():
    company_details_table = db.Model.metadata.tables['CompanyDetails']
    tickers_table = db.Model.metadata.tables['Tickers']

    # Get the ticker_name query parameter as a list
    ticker_list = request.args.getlist('ticker_name')

    # Columns to exclude
    columns_to_exclude = set([
        'tickerId',
        'companyDetailsId',
        # ... other columns to exclude
    ])

    # Dynamically determine columns to select
    columns_to_select = [
        column for column in company_details_table.c if column.name not in columns_to_exclude
    ]

    # Add tickerName from tickers_table
    columns_to_select.append(tickers_table.c.tickerName)

    # Construct the SELECT statement with necessary joins and filters
    if ticker_list:
        select_statement = select(*columns_to_select).select_from(
            company_details_table.join(tickers_table, company_details_table.c.tickerId == tickers_table.c.tickerId)
        ).where(tickers_table.c.tickerName.in_(ticker_list))
    else:
        select_statement = select(*columns_to_select).select_from(
            company_details_table.join(tickers_table, company_details_table.c.tickerId == tickers_table.c.tickerId)
        )

    try:
        with db.engine.connect() as connection:
            result = connection.execute(select_statement)
            company_details_list = [row._asdict() for row in result]
        return jsonify(company_details_list)
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
