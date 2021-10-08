from datetime import datetime, timedelta
from collections import defaultdict
import random

import pymongo
from flask import Flask
from flask_pymongo import PyMongo

from .names import first_names, last_names
from .objects import Airline, Airport, Flight, Seat, Booking, Person

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://localhost:27017/myDatabase"
mongo = PyMongo(app)


def random_date(start=datetime(1930, 1, 1), end=datetime(2020, 1, 1)):
  """Generate a random datetime between `start` and `end`"""
  return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def populate_bookings(db):
    person_id = 0
    for flight in db["flights"]:
        available_seats = list(db["seats"][flight.flight_id])
        fill_seats = random.randint(0, len(available_seats)-1)
        selected_seats = random.sample(available_seats, k=fill_seats)
        # print(f"Bookings\t{flight.flight_id}\t{fill_seats / len(available_seats):.2f}")
        for seat_id, (first_name, last_name) in zip(selected_seats, zip(random.choices(first_names(), k=fill_seats), random.choices(last_names(), k=fill_seats))):
            seat = db["seats"][flight.flight_id][seat_id]
            seat.booked = True
            person = Person(person_id, f"{first_name} {last_name}", random_date(), str(random.randint(10000000, 100000000-1)), seat.travel_class)
            person_id += 1
            db["persons"].append(person)
            db["bookings"].append(Booking(seat_id=seat.seat_id, person_id=person.person_id))
    return person_id

def generate_dataset():
    airplanes = list({
        "Airbus A220": {"rows": 36, "cols": 6},
        "Boeing 777": {"rows": 39, "cols": 8},
    }.items())

    airlines = {
        "Swiss": (Airline(0, "Swiss", "https://seeklogo.net/wp-content/uploads/2017/02/swiss-international-air-lines-logo.png"), 1.09),
        "KLM": (Airline(1, "KLM", "https://upload.wikimedia.org/wikipedia/commons/thumb/c/c7/KLM_logo.svg/500px-KLM_logo.svg.png"), 0.89),
        "Austrian": (Airline(2, "Austrian", "https://upload.wikimedia.org/wikipedia/commons/thumb/c/c2/Austrian_Airlines%27_logo_%282018%29.png/800px-Austrian_Airlines%27_logo_%282018%29.png"), 1.01),
        "Delta": (Airline(3, "Delta", "https://1000logos.net/wp-content/uploads/2017/09/Delta-Air-Lines-Logo.png"), 0.95),
    }

    airports = {
        "ZRH": {"obj": Airport("ZRH", "Zurich", "Switzerland", []), "price_modifier": 1.5, "airlines": ["Swiss", "Austrian", "Delta"]},
        "VIE": {"obj": Airport("VIE", "Vienna", "Austria", []), "price_modifier": 1.1, "airlines": ["Austrian", "KLM"]},
        "SYD": {"obj": Airport("SYD", "Syndey", "Australia", []), "price_modifier": 1.3, "airlines": ["Swiss", "Delta"]},
        "LHR": {"obj": Airport("LHR", "London", "England", []), "price_modifier": 1.2, "airlines": ["Austrian", "Swiss", "KLM"]},
        "OTP": {"obj": Airport("OTP", "Bucharest", "Romania", []), "price_modifier": 0.8, "airlines": ["Swiss"]},
        "JFK": {"obj": Airport("JFK", "New York City", "United States of America", []), "price_modifier": 1.4, "airlines": ["Swiss", "KLM", "Delta"]},
    }

    distance_matrix = {
        "ZRH": [0.0,  1.0,  15.0, 1.0,  2.3,  9.0],
        "VIE": [1.0,  0.0,  14.0, 2.0,  1.3,  10.0],
        "SYD": [15.0, 14.0, 0.0,  16.0, 13.0, 9.0],
        "LHR": [1.0,  2.0,  16.0, 0.0,  3.0,  8.0],
        "OTP": [2.3,  1.3,  13.0, 3.0,  0.0,  11.5],
        "JFK": [9.0,  10.0, 9.0,  8.0,  11.5, 0.0],
    }

    prices = {
        1: {"rows": set(range(0, 9)), "price_modifier": 1.9},
        2: {"rows": set(range(9, 100000)), "price_modifier": 0.9},
    }
    price_per_hour = 50.0

    years = [2020]
    months = [1]
    days = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10:31, 11: 30, 12: 31}
    seat_id = 0

    db = {
        "airlines": [a[0] for a in airlines.values()],
        "airports": [a["obj"] for a in airports.values()],
        "flights": [],
        "seats": defaultdict(lambda: {}),
        "bookings": [],
        "persons": [],
    }
    for src, src_obj in airports.items():
        for i, (dst, dst_obj) in enumerate(airports.items()):
            for airline in src_obj["airlines"]:
                airline_obj, airline_modifier = airlines[airline]
                duration = distance_matrix[src][i] * (1+((random.random()/2)**4))
                duration_mins = int(duration * 60)
                if src == dst or airline not in dst_obj["airlines"]:
                    continue
                if random.random() < 0.25:
                    continue
                price = duration * price_per_hour * src_obj["price_modifier"] * dst_obj["price_modifier"] * airline_modifier
                print(f"{src}->{dst} ({airline}, {duration_mins / 60:.2f} hrs, 1st price {int(price * prices[1]['price_modifier'])} EUR, 2nd price {int(price * prices[2]['price_modifier'])} EUR)")

                for year in years:
                    for month in months:
                        for day in range(1, days[month]+1):
                            hour = random.randint(7, 21)
                            minute = random.randint(0, 59)
                            date_str = "-".join(str(x) for x in [year, month, day, hour, minute])
                            date = datetime.strptime(date_str, "%Y-%m-%d-%H-%M")

                            plane, seat_cnt = random.choice(airplanes)
                            f = Flight(f"{src}_{dst}_{airline}_{date_str}", airline_obj.airline_id, src_obj["obj"].airport_id, dst_obj["obj"].airport_id, plane, date, duration_mins)
                            db["flights"].append(f)

                            for row in range(1, seat_cnt["rows"]+1):
                                travel_class, price_modifier = [p for p in prices.items() if row in p[1]["rows"]][0]
                                price_modifier = price_modifier["price_modifier"]
                                for col in range(seat_cnt["cols"]):
                                    seat = f"{row}{chr(ord('A') + col)}"
                                    db["seats"][f.flight_id][seat_id] = Seat(seat_id, f.flight_id, seat, travel_class, price * price_modifier, booked=False)
                                    seat_id += 1
    return db


def check_insert_many(collection, inserted_vals):
  result = collection.insert_many(a.to_dict() for a in inserted_vals)
  if len(result.inserted_ids) != len(inserted_vals):
    raise ValueError(f"Tried to insert: {len(inserted_vals)}, inserted: {len(result.inserted_ids)}")


def populate_db():
  def print_db(db):
    for s in ["airports", "airlines", "flights", "seats", "bookings", "persons"]:
      if s == "seats":
        print(s, sum(len(l) for l in db[s].values()))
      else:
        print(s, len(db[s]))

  print("Start populating database.")
  db_dict = generate_dataset()
  print("Start populating bookings.")
  populate_bookings(db_dict)

  print("\nAdding to database:")
  print_db(db_dict)

  mongo.db.airports.drop()
  mongo.db.airlines.drop()
  mongo.db.flights.drop()
  mongo.db.seats.drop()
  mongo.db.bookings.drop()
  mongo.db.persons.drop()

  mongo.db.airports.create_index([("airport_id", pymongo.ASCENDING)], unique=True)
  mongo.db.airlines.create_index([("airline_id", pymongo.ASCENDING)], unique=True)
  mongo.db.flights.create_index([("flight_id", pymongo.ASCENDING)], unique=True)
  mongo.db.seats.create_index([("seat_id", pymongo.ASCENDING)], unique=True)
  mongo.db.bookings.create_index([("seat_id", pymongo.ASCENDING), ("person_id", pymongo.ASCENDING)], unique=True)
  mongo.db.persons.create_index([("person_id", pymongo.ASCENDING)], unique=True)

  check_insert_many(mongo.db.airports, db_dict["airports"])
  check_insert_many(mongo.db.airlines, db_dict["airlines"])
  check_insert_many(mongo.db.flights, db_dict["flights"])
  check_insert_many(mongo.db.seats, [b for a in db_dict["seats"].values() for b in a.values()])
  check_insert_many(mongo.db.bookings, db_dict["bookings"])
  check_insert_many(mongo.db.persons, db_dict["persons"])
  print("\nFinished adding to database.")
    

if __name__ == "__main__":
    populate_db()
