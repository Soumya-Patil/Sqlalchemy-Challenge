# Import ORM and Dependencies
from flask import Flask, jsonify
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datetime as dt
import numpy as np


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine , reflect = True)

# We can view all of the classes that automap found
Base.classes.keys()

# Save references to each table
measurement_obj = Base.classes.measurement
station_obj = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    print("Server received request for 'Home' page...")
    return(
        f"Welcome to my Home Page! <br/>"
        f"Below are the Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/temp/start/end"
    )

# Create our session (link) from Python to the DB
session = Session(engine)


@app.route("/api/v1.0/precipitation")
def precipitation():
#  based on the queries that were developed previously in the Jupyter Notebook
# Calculate the date 1 year ago from the last data point in the database
    last_year = dt.date(2017 , 8 ,23) - dt.timedelta(days = 365)

# Perform a query to retrieve the date and precipitation scores
    query_result = session.query(measurement_obj.date, measurement_obj.prcp).\
               filter(measurement_obj.date >= last_year).all()

 # place the date as key , and prcp as value, off of the result and JSONify it
    date_prcp_results = {date: prcp for date, prcp in query_result}
    return jsonify(date_prcp_results)


@app.route("/api/v1.0/stations")
def stations():
   # return a JSON list of stations from the dataset.
   stations_query = session.query(station_obj.station).all()

   # Convert list of tuples into normal list
   stations_result = list(np.ravel(stations_query))
   return jsonify(stations_result)


@app.route("/api/v1.0/tobs")
def stations_temp():

    #lets calculate the most active station first
    
    stations = session.query(measurement_obj,station_obj).\
                 filter(measurement_obj.station == station_obj.station).all()

    no_of_stations = []
    for record in stations:
        (x,y) = record
        no_of_stations.append(y.station)
    
    from collections import Counter
    active_stations = Counter(no_of_stations)
    

    # List the stations and the counts in descending order.
    most_active_desc = active_stations.most_common()
    most_active_station = most_active_desc[0][0]

    # Calculate what will be the last year 
    last_year = dt.date(2017 , 8 ,23) - dt.timedelta(days = 365)

    # now query for the temps for the above year for the most_active_station
    tobs_query = session.query(measurement_obj.tobs).\
                 filter(measurement_obj.station == most_active_station).\
                 filter(measurement_obj.date >= last_year).all()

    # Convert list of tuples into normal list
    station_temp = list(np.ravel(tobs_query))

    #JSONify the list
    return jsonify(station_temp)



















