import dataclasses
from typing import List, Tuple
from datetime import datetime, timedelta

##### Local classes

class Persistable:
  def to_dict(self):
    return dataclasses.asdict(self)

  @staticmethod
  def from_dict(d):
    raise NotImplementedError()

  def load(self, db):
    raise NotImplementedError() 

##### Persisted classes

@dataclasses.dataclass
class Person(Persistable):
  person_id: int
  name: str
  birthdate: datetime
  passport: str
  travel_class: int

  @staticmethod
  def from_dict(d):
    return Person(int(d["person_id"]), d["name"], d["birthdate"], d["passport"], int(d["travel_class"]))

  def load(self, db):
    return self

@dataclasses.dataclass
class Airline(Persistable):
  airline_id: int
  name: str
  logo_url: str

  @staticmethod
  def from_dict(d):
    return Airline(int(d["airline_id"]), d["name"], d["logo_url"])

  def load(self, db):
    return self

@dataclasses.dataclass
class Airport(Persistable):
  airport_id: str
  city: str
  country: str
  keywords: List[str]

  @staticmethod
  def from_dict(d):
    return Airport(d["airport_id"], d["city"], d["country"], d["keywords"])

  def load(self, db):
    return self

@dataclasses.dataclass
class Flight(Persistable):
  flight_id: str
  airline_id: int
  departure_airport_id: str
  arrival_airport_id: str
  plane: str
  date: datetime
  duration_mins: int

  @staticmethod
  def from_dict(d):
    return Flight(d["flight_id"], int(d["airline_id"]), d["departure_airport_id"], d["arrival_airport_id"], d["plane"], d["date"], int(d["duration_mins"]))

  def load(self, db):
    self.airline = Airline.from_dict(db.airlines.find({"airline_id": self.airline_id}).next()).load(db)
    self.departure_airport = Airport.from_dict(db.airports.find({"airport_id": self.departure_airport_id}).next()).load(db)
    self.arrival_airport = Airport.from_dict(db.airports.find({"airport_id": self.arrival_airport_id}).next()).load(db)
    return self

  @staticmethod
  def __get_time_as_str(dt: datetime) -> str:
    return dt.strftime("%Y-%-m-%d %H:%M")

  @property
  def departure(self) -> str:
    return Flight.__get_time_as_str(self.date)

  @property
  def arrival(self) -> str:
    return Flight.__get_time_as_str(self.date + timedelta(minutes=self.duration_mins))


@dataclasses.dataclass
class Seat(Persistable):
  seat_id: int
  flight_id: str
  number: str
  travel_class: int
  price: int
  booked: bool

  @staticmethod
  def from_dict(d):
    return Seat(int(d["seat_id"]), d["flight_id"], d["number"], d["travel_class"], int(d["price"]), bool(d["booked"]))

  def load(self, db):
    self.flight = Flight.from_dict(db.flights.find({"flight_id": self.flight_id}).next()).load(db)
    return self

@dataclasses.dataclass
class Booking(Persistable):
  seat_id: int
  person_id: int

  @staticmethod
  def from_dict(d):
    return Booking(int(d["seat_id"]), int(d["person_id"]))

  def load(self, db):
    self.seat = Seat.from_dict(db.seats.find({"seat_id": self.seat_id})).load(db)
    self.person = Person.from_dict(db.person.find({"person_id": self.person_id})).load(db)
    return self
