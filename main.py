from typing import Any, List, Dict

import datetime
import json

from flask import Flask, render_template, request, redirect
from flask_pymongo import PyMongo
from flask_bootstrap import Bootstrap
from flask_qrcode import QRcode

from bson.code import Code
from bson.son import SON
import pymongo

from .objects import Airline, Airport, Flight, Person, Booking, Seat

app = Flask(__name__)
Bootstrap(app)
QRcode(app)
app.config["MONGO_URI"] = "mongodb://localhost:27017/myDatabase"
mongo = PyMongo(app)

if "persons" in mongo.db.list_collection_names():
  person_id = mongo.db.persons.find().sort("person_id", pymongo.DESCENDING).limit(1)[0]["person_id"] + 1
else:
  person_id = 0

def compute_occupancy(flight_ids: List[str]) -> Dict[str, float]:
  occupancy = mongo.db.seats.map_reduce(
    Code("""function () {
      emit(this.flight_id, { booked: this.booked });
    }"""), Code("""function (key, values) {
      var results = {"flight_id": key, "occupancy": 0.0};
      occupied = 0;
      total = 0;
      values.forEach(function (value) {
        total++;
        if (value["booked"] === true) occupied++;
      });
      results["occupancy"] = occupied / total;
      return results;
    }"""), "occupancy", query={
      "flight_id": {"$in": flight_ids}
    })
  occupancy = {v["value"]["flight_id"]: v["value"]["occupancy"] for v in occupancy.find()}
  return occupancy


def get_seats(db: Any, travel_class: int, matcher: Any):
  found_flights = db.flights.aggregate([
    matcher,
    { "$lookup": {
        "from": "seats", 
        "localField": "flight_id", 
        "foreignField": "flight_id", 
        "as": "seat"
      }
    },
    { "$unwind": "$seat" },
    { "$group": {
        "_id": "$flight_id",
        "flight": { "$first": "$$CURRENT" },
        "seats": { "$push": "$seat" }, 
      }
    }
  ])

  seats = []
  for s in found_flights:
    seat = None
    for seat_dict in s["seats"]:
      if not seat_dict["booked"] and seat_dict["travel_class"] == travel_class:
        seat = Seat.from_dict(seat_dict)
        break
    if seat is None:
      continue  # Skip flight if no available seats.  
    
    seat.flight = Flight.from_dict(s["flight"]).load(db)
    seats.append(seat)
  print(f"Found {len(seats)} flights.")
  return seats


@app.route('/')
def find_flight():
  return render_template('index.html')


@app.route('/search', methods = ["POST"])
def search():
  name = request.values["pass_name"]
  birthdate = datetime.datetime.strptime(request.values["pass_birthdate"], "%Y-%m-%d")
  travel_class = int(request.values["pass_class"])
  passport = request.values["pass_passport"]
  
  global person_id
  person = Person(person_id, name, birthdate, passport, travel_class)
  person_id += 1
  mongo.db.persons.insert_one(person.to_dict())

  src_airport = request.values["from"]
  dst_airport = request.values["to"]
  dep_date = request.values["dep_date"]
  dep_time = request.values["dep_time"]
  
  dep_datetime = datetime.datetime.strptime(f"{dep_date} {dep_time}", "%Y-%m-%d %H:%S")
  next_day = dep_datetime + datetime.timedelta(hours=48)
  print("Flights between", dep_datetime, next_day)
  
  seats = get_seats(mongo.db, person.travel_class, matcher={ "$match": {
      "departure_airport_id": src_airport,
      "arrival_airport_id": dst_airport,
      "date": {"$lte": next_day, "$gte": dep_datetime}
    }
  })
  seats.sort(key=lambda s: s.flight.date)

  flight_ids = [s.flight_id for s in seats]
  variables = {
      "seats": seats,
      "occupancy": compute_occupancy(flight_ids),
      "person": person,
  }
  return render_template('search.html', **variables)


def render_boarding_pass(base_url: str, person: Person, seat: Seat):
  variables = {
      "name": person.name,
      "date": seat.flight.date.strftime("%Y-%m-%d"),
      "time": seat.flight.date.strftime("%H:%M"),
      "passport_no": person.passport,
      "airline_name": seat.flight.airline.name,
      "seat": seat.number,
      "seat_id": seat.seat_id,
      "base_url": base_url,
      "person_id": person.person_id,
  }
  return render_template('boarding_pass.html', **variables)


@app.route("/boarding_pass/<int:seat_id>/<int:person_id>")
def boarding_pass(seat_id: int, person_id: int):
  booking = Booking.from_dict(mongo.db.bookings.find({"person_id": person_id, "seat_id": seat_id})[0]).load(mongo.db)
  return render_boarding_pass(request.base_url, booking.person, booking.seat)


@app.route('/book/<int:seat_id>/<int:person_id>')
def book(seat_id: int, person_id: int):
  person_query = mongo.db.persons.find({"person_id": person_id})
  num_persons = person_query.count()
  if num_persons != 1:
    raise ValueError(f"Found {num_persons} people with {person_id}.")
  
  person = Person.from_dict(person_query[0]).load(mongo.db)
  booking = Booking(seat_id=seat_id, person_id=person_id)

  update_result = mongo.db.seats.update_one({"seat_id": seat_id, "booked": False}, { "$set": {"booked": True}})
  if update_result.modified_count != 1:
    return render_template("seat_booking_failed.html", seat_id=seat_id)
  mongo.db.bookings.insert_one(booking.to_dict())

  seat = Seat.from_dict(mongo.db.seats.find({"seat_id": seat_id})[0]).load(mongo.db)
  return render_boarding_pass(request.base_url, person, seat)


@app.route('/best')
def best():
    return render_template('best.html')


@app.route('/search_best', methods = ["POST"])
def search_best():
  travel_class = int(request.values["pass_class"])
  
  src_airport = request.values["from"]
  dep_date = request.values["dep_date"]
  dep_time = "00:00"

  dep_datetime = datetime.datetime.strptime(f"{dep_date} {dep_time}", "%Y-%m-%d %H:%S")
  next_day = dep_datetime + datetime.timedelta(hours=48)
  print("Best flights between", dep_datetime, next_day)
  
  seats = get_seats(mongo.db, travel_class, matcher={ "$match": {
      "departure_airport_id": src_airport,
      "date": {"$lte": next_day, "$gte": dep_datetime}
    }
  })
  seats.sort(key=lambda s: (s.price, s.flight.date))
  print(f"Found {len(seats)} flights.")

  flight_ids = [s.flight_id for s in seats]
  variables = {
      "seats": seats,
      "occupancy": compute_occupancy(flight_ids),
  }
  return render_template('search.html', **variables)

@app.route('/airlines')
def airlines():
  # mongo.db.flight_seats.drop()
  # mongo.db.flights.aggregate([
  #   # { "$match": {
  #   #     "airline_id": airline_id
  #   #   }
  #   # },
  #   { "$lookup": {
  #       "from": "airlines", 
  #       "localField": "airline_id", 
  #       "foreignField": "airline_id", 
  #       "as": "airline"
  #     }
  #   },
  #   { "$unwind": "$airline" },
  #   { "$lookup": {
  #       "from": "seats", 
  #       "localField": "flight_id", 
  #       "foreignField": "flight_id", 
  #       "as": "seat"
  #     }
  #   },
  #   { "$unwind": "$seat" },
  #   { "$group": {
  #       "_id": "$seat_id",
  #       "flight": { "$first": "$$CURRENT" },
  #       "seats": { "$push": "$seat" }, 
  #       "airline": { "$first": "$airline" },
  #     }
  #   },
  #   { "$out": "flight_seats"}
  # ],  allowDiskUse=True)

  reduce_oc = Code("""function (key, values) {
      var results = {};
      var occupied = 0;
      var total = 0;
      values.forEach(function (value) {
        if (values["airline_id"] !== undefined) results["airline_id"] = values["airline_id"];
        if (values["booked"] !== undefined) {
          total++;
          occupied += value["booked"];
        }
      });
      results["total"] = total;
      results["occupied"] = occupied;
      return results;
    }""")
  mongo.db.seats.map_reduce(Code("""function () {
      emit(this.flight_id, { booked: this.booked ? 1 : 0 });
    }"""), reduce_oc, out={ "reduce": "occupancy"})
  mongo.db.flights.map_reduce(Code("""function () {
    emit(this.flight_id, { airline_id: this.airline_id });
  }"""), reduce_oc, out={"reduce": "occupancy"})
  for k in mongo.db.occupancy.find():
    print(k)
  occupancy = {v["value"]["airline_id"]: v["value"]["occupancy"] for v in occupancy.find()}
  airline_ids = occupancy.keys()

  airports = mongo.db.flight_seats.map_reduce(
    Code("""function () {
      emit(this.airline.airline_id, { airport: this.departure_airport_id });
    }"""), Code("""function (key, values) {
      var set = new Set();
      values.forEach(function (value) {
        set.add(value["airport_id"]);
      });
      var results = {"airline_id": key, "airports": set.size};
      return results;
    }"""), "airports")
  airports = {v["value"]["airline_id"]: v["value"]["airports"] for v in airports.find()}

  price = mongo.db.flight_seats.map_reduce(
  Code("""function () {
      var price_sum = 0;
      var seats = 0;
      this.seats.forEach(function (value) {
        price_sum += value["price"];
        seats++;
      });
      emit(this.airline.airline_id, { price: price_sum, seats: seats });
  }"""), Code("""function (key, values) {
    var price_sum = values.reduce((a, b) => a["price"] + b["price"], 0);
    var seat_sum = values.reduce((a, b) => a["seats"] + b["seats"], 0);
    var results = {"airline_id": key, "price": price_sum / seat_sum};
    return results;
  }"""), "price")
  price = {v["value"]["airline_id"]: v["value"]["price"] for v in price.find()}

  return render_template('airlines.html', {
    "s": [{
      "airline": result["airline"]["airline_id"],
      "occupancy": occupancy[result["airline"]["airline_id"]],
      "airports_served": airports[result["airline"]["airline_id"]],
      "avg_price": price[result["airline"]["airline_id"]],
    } for result in found_flights.find()]
  })


if __name__ == '__main__':
    app.run(debug=True)
