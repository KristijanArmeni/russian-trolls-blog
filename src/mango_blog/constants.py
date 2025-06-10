import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

DATES = {
    "brussels": datetime(year=2016, month=3, day=22),
    "chelsea": datetime(year=2016, month=9, day=17),  # march 22
    "first_debate": datetime(year=2016, month=9, day=26),
    "election": datetime(year=2016, month=11, day=8),
}

DATES2FORMATTED = {
    "brussels": "Bombing in Brussels",
    "chelsea": "Bombing in Chelsea, NYC",
    "election": "US election day",
    "first_debate": "First presidential debate",
}

load_dotenv()

DATA_PATH = os.getenv("DATA_PATH")
OUTPUT_PATH = Path(DATA_PATH, "outputs")
DATASET_FNAME = "confirmed_russia_troll_tweets.csv"
