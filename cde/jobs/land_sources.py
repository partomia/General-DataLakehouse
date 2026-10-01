"""
Stage 0 - Landing: five synthetic source systems drop their files for one business date.

  cbs        core banking on MySQL. First business date: a mysqldump-style .sql file
             (CREATE TABLE + extended INSERTs) of branch, product, customer, account.
             Later dates: Debezium-style CDC events, one JSON line each (op c/u/d,
             before, after, ts_ms, source.file/pos). Every date: an end-of-day
             balance CSV with a trailer record.                        (structured)
  lms        loan management: borrower, loan (daily full extract) and repayment
             CSVs, pipe-delimited, dd/MM/yyyy dates, header + trailer.  (structured)
  payments   payments hub: nested JSON lines (debtor / creditor / amount / charges);
             a `device` object appears from the fourth date (schema drift). (semi)
  crm        CRM / digital onboarding: a JSON array of customer documents with
             identifier and address arrays.                               (semi)
  documents  correspondence e-mails (.eml), KYC declarations (.txt) and ID scans
             (.png).                                                      (unstructured)

Each <landing>/<source>/<date>/ folder gets _manifest.json: per entity the
record count and control total the source itself reports, and per file its size
and sha256. Reconciliation is against this manifest.

The world is simulated from a seed, from the first business date on, so every
run gives the same files and a date can be (re)landed on its own. The same
person appears in several systems with typos, initials, formatted phones and old
addresses; a few households share a phone and two namesakes share a name and a
date of birth (MDM must keep those apart). Faults are planted for validation and
reconciliation: missing mandatory fields, impossible dates, bad PANs, re-sent
duplicates, out-of-order and unknown-key CDC events, a wrong CSV trailer, an
empty document, an unknown currency. _truth/persons.json records which source
records are the same person, so MDM can be scored.

Usage:
  spark-submit land_sources.py --business-date 2026-09-21 [--landing URI] [--customers N]
  python cde/jobs/land_sources.py --all-dates --landing data/landing      # laptop, no Spark
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import math
import random
import struct
import sys
import zlib
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gdl_common as C  # noqa: E402

CFG = C.load_json("config/pipeline.json")
DATES = [date.fromisoformat(d) for d in CFG["business_dates"]]
BANK_IFSC = "GDLB"

FIRST_M = ["Aarav", "Vivaan", "Aditya", "Arjun", "Rohan", "Rahul", "Amit", "Suresh", "Ramesh", "Vikram",
           "Sanjay", "Rajesh", "Anil", "Manoj", "Deepak", "Kiran", "Ravi", "Sunil", "Ajay", "Nitin", "Prakash",
           "Mohammed", "Imran", "Farhan", "Joseph", "Thomas", "Gurpreet", "Harpreet", "Venkatesh", "Srinivas",
           "Karthik", "Arun", "Ganesh", "Mahesh", "Naveen"]
FIRST_F = ["Priya", "Anjali", "Sneha", "Pooja", "Neha", "Kavita", "Sunita", "Anita", "Lakshmi", "Meena",
           "Divya", "Shreya", "Aishwarya", "Fatima", "Ayesha", "Mary", "Simran", "Harleen", "Lavanya", "Deepa",
           "Revathi", "Swati", "Nisha", "Rekha", "Geeta"]
LAST = ["Sharma", "Verma", "Gupta", "Singh", "Kumar", "Patel", "Shah", "Mehta", "Reddy", "Rao", "Naidu", "Iyer",
        "Nair", "Menon", "Pillai", "Das", "Banerjee", "Chatterjee", "Mukherjee", "Ghosh", "Bose", "Khan",
        "Shaikh", "Qureshi", "Dsouza", "Fernandes", "Joshi", "Kulkarni", "Deshpande", "Patil", "Jadhav",
        "Chauhan", "Yadav", "Mishra", "Pandey", "Tiwari", "Agarwal", "Jain", "Bhat", "Hegde", "Gill", "Sandhu"]
# city, state, pin prefix, region (one branch per city)
CITIES = [("Mumbai", "Maharashtra", "400", "WEST"), ("Pune", "Maharashtra", "411", "WEST"),
          ("Ahmedabad", "Gujarat", "380", "WEST"), ("Delhi", "Delhi", "110", "NORTH"),
          ("Jaipur", "Rajasthan", "302", "NORTH"), ("Lucknow", "Uttar Pradesh", "226", "NORTH"),
          ("Chennai", "Tamil Nadu", "600", "SOUTH"), ("Bengaluru", "Karnataka", "560", "SOUTH"),
          ("Hyderabad", "Telangana", "500", "SOUTH"), ("Kochi", "Kerala", "682", "SOUTH"),
          ("Kolkata", "West Bengal", "700", "EAST"), ("Bhubaneswar", "Odisha", "751", "EAST")]
STREETS = ["MG Road", "Station Road", "Park Street", "Nehru Nagar", "Gandhi Marg", "Lake View Road",
           "Church Street", "Civil Lines", "Shivaji Nagar", "Anna Salai", "Residency Road", "Brigade Road",
           "Linking Road", "Ring Road", "Tilak Nagar", "Market Street"]
LANDMARKS = ["City Hospital", "Central Library", "Bus Depot", "Ram Mandir", "St. Mary's Church", "Railway Station",
             "Post Office", "Municipal School", "Water Tank", "Police Station"]
# product_code, name, type, domain, base rate %
PRODUCTS = [("SA01", "Savings Account - Regular", "SA", "DEPOSITS", 2.70),
            ("SA02", "Savings Account - Salary", "SA", "DEPOSITS", 3.00),
            ("CA01", "Current Account", "CA", "DEPOSITS", 0.00),
            ("TD01", "Fixed Deposit 1 Year", "TD", "DEPOSITS", 6.80),
            ("TD02", "Fixed Deposit 3 Years", "TD", "DEPOSITS", 7.10),
            ("HL01", "Home Loan", "HL", "LENDING", 8.50),
            ("PL01", "Personal Loan", "PL", "LENDING", 11.25),
            ("AL01", "Auto Loan", "AL", "LENDING", 9.10),
            ("BL01", "Business Loan", "BL", "LENDING", 10.40)]
RATE = {p[0]: p[4] for p in PRODUCTS}
OTHER_BANKS = ["HDFC", "ICIC", "SBIN", "UTIB", "KKBK", "PUNB"]
LOAN_SPEC = {  # product -> (min, max sanction INR, tenure months)
    "HL01": (1_500_000, 8_000_000, 240), "PL01": (100_000, 1_000_000, 48),
    "AL01": (300_000, 1_500_000, 60), "BL01": (500_000, 5_000_000, 84)}
CHANNEL_FEE = {"NEFT": 2.50, "IMPS": 5.00, "RTGS": 25.00, "SWIFT": 500.00}

CBS_TABLES = {  # table -> [(column, mysql type)], primary key first
    "branch": [("branch_code", "varchar(6)"), ("ifsc", "varchar(11)"), ("branch_name", "varchar(80)"),
               ("city", "varchar(40)"), ("state", "varchar(40)"), ("region", "varchar(10)"),
               ("opened_on", "date")],
    "product": [("product_code", "varchar(6)"), ("product_name", "varchar(80)"), ("product_type", "varchar(4)"),
                ("domain", "varchar(12)"), ("base_rate", "decimal(5,2)")],
    "customer": [("cust_id", "int"), ("title", "varchar(5)"), ("first_name", "varchar(40)"),
                 ("middle_name", "varchar(40)"), ("last_name", "varchar(40)"), ("dob", "date"),
                 ("gender", "char(1)"), ("pan", "varchar(10)"), ("aadhaar", "varchar(12)"),
                 ("mobile", "varchar(16)"), ("email", "varchar(80)"), ("addr_line1", "varchar(120)"),
                 ("addr_line2", "varchar(120)"), ("city", "varchar(40)"), ("state", "varchar(40)"),
                 ("pincode", "varchar(6)"), ("kyc_status", "varchar(10)"), ("segment", "varchar(10)"),
                 ("home_branch", "varchar(6)"), ("created_at", "datetime"), ("updated_at", "datetime")],
    "account": [("acct_no", "varchar(14)"), ("cust_id", "int"), ("joint_cust_id", "int"),
                ("product_code", "varchar(6)"), ("branch_code", "varchar(6)"), ("currency", "char(3)"),
                ("open_date", "date"), ("status", "varchar(10)"), ("close_date", "date"),
                ("interest_rate", "decimal(5,2)"), ("updated_at", "datetime")],
}
EOD_COLS = ["acct_no", "bal_date", "ledger_balance", "available_balance", "currency"]
BORROWER_COLS = ["borrower_id", "full_name", "dob", "pan", "mobile", "address", "city", "pincode",
                 "cbs_cust_ref", "created_on", "updated_on"]
LOAN_COLS = ["loan_id", "borrower_id", "product_code", "branch_code", "sanction_date", "sanction_amount",
             "interest_rate", "tenure_months", "emi_amount", "principal_outstanding", "principal_overdue",
             "interest_overdue", "dpd", "last_payment_date", "loan_status", "src_asset_class",
             "restructured_flag", "loss_flag", "as_of_date"]
REPAYMENT_COLS = ["txn_id", "loan_id", "payment_date", "amount", "mode"]


def R(seed: int, label: str) -> random.Random:
    return random.Random(f"{seed}:{label}")


def dmy(d: date | None) -> str:
    return d.strftime("%d/%m/%Y") if d else ""


def ist(d: date, rng: random.Random, start_h: int = 9, end_h: int = 18) -> datetime:
    return datetime(d.year, d.month, d.day) + timedelta(seconds=rng.randint(start_h * 3600, end_h * 3600))


def epoch_ms(local_ist: datetime) -> int:
    return calendar.timegm((local_ist - timedelta(hours=5, minutes=30)).timetuple()) * 1000


def sql_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def months_between(a: date, b: date) -> int:
    return (b.year - a.year) * 12 + b.month - a.month


def add_months(d: date, m: int) -> date:
    y, mo = divmod(d.month - 1 + m, 12)
    return date(d.year + y, mo + 1, min(d.day, 28))


def emi(principal: float, rate: float, n: int) -> float:
    r = rate / 1200
    return principal * r * (1 + r) ** n / ((1 + r) ** n - 1)


def balance_after(principal: float, rate: float, n: int, k: int) -> float:
    r, e = rate / 1200, emi(principal, rate, n)
    return max(0.0, principal * (1 + r) ** k - e * ((1 + r) ** k - 1) / r)


def typo(rng: random.Random, s: str) -> str:
    if len(s) < 4:
        return s
    i = rng.randint(1, len(s) - 3)
    kind = rng.random()
    if kind < 0.4:
        return s[:i] + s[i + 1] + s[i] + s[i + 2:]
    if kind < 0.7:
        return s[:i] + s[i] + s[i:]
    return s[:i] + s[i + 1:]


def abbreviate(line: str) -> str:
    for a, b in (("Road", "Rd."), ("Street", "St."), ("Nagar", "Ngr"), ("Marg", "Mg."), ("Near", "Nr.")):
        line = line.replace(a, b)
    return line


def fmt_mobile(rng: random.Random, m: str) -> str:
    style = rng.random()
    if style < 0.35:
        return m
    if style < 0.6:
        return f"+91-{m[:5]} {m[5:]}"
    if style < 0.8:
        return f"0{m}"
    return f"+91 {m}"


@dataclass
class Person:
    pid: int
    first: str
    middle: str
    last: str
    gender: str
    dob: date
    pan: str
    aadhaar: str
    mobile: str
    email: str
    line1: str
    line2: str
    city: str
    state: str
    pincode: str
    region: str
    segment: str
    relation: str = ""          # household:<pid> or namesake:<pid>
    systems: dict = field(default_factory=lambda: {"cbs": [], "lms": [], "crm": []})


class World:
    """Every source system's state, simulated date by date from the first business date."""

    def __init__(self, customers: int = CFG["customers"], seed: int = CFG["seed"]):
        self.n, self.seed = customers, seed
        self.persons: list[Person] = []
        self.pans: set[str] = set()
        self.mobiles: set[str] = set()
        self.branches = [(f"BR{i + 101:03d}", f"{BANK_IFSC}0{i + 101:06d}", f"{c} Main Branch", c, s, reg,
                          date(2005 + i % 9, 1 + i % 12, 1)) for i, (c, s, _, reg) in enumerate(CITIES)]
        self.branch_by_city = {b[3]: b for b in self.branches}
        self.customers: dict[int, dict] = {}      # cust_id -> current CBS row
        self.cust_pid: dict[int, int] = {}
        self.accounts: dict[str, dict] = {}       # acct_no -> current CBS row
        self.balances: dict[str, float] = {}
        self.borrowers: dict[str, dict] = {}
        self.loans: dict[str, dict] = {}
        self.crm: dict[str, dict] = {}
        self.out: dict[date, dict] = {}
        self._cust_seq, self._acct_seq, self._borr_seq, self._loan_seq = 100001, 1, 200001, 1
        self._build()
        prev = None
        for d in DATES:
            self.out[d] = self._day(d, prev)
            prev = d

    # ------------------------------------------------------------ people

    def _new_pan(self, rng, last: str) -> str:
        while True:
            pan = ("".join(rng.choice("ABCDEFGHJKLMNPRSTUVWXYZ") for _ in range(3)) + "P" + last[0].upper()
                   + f"{rng.randint(0, 9999):04d}" + rng.choice("ABCDEFGHJKLMNPRSTUVWXYZ"))
            if pan not in self.pans:
                self.pans.add(pan)
                return pan

    def _new_mobile(self, rng) -> str:
        while True:
            m = rng.choice("6789") + f"{rng.randint(0, 999_999_999):09d}"
            if m not in self.mobiles:
                self.mobiles.add(m)
                return m

    def _address(self, rng, city=None):
        c, s, pin, reg = city or rng.choice(CITIES)
        return (f"{rng.randint(1, 250)}, {rng.choice(STREETS)}", f"Near {rng.choice(LANDMARKS)}",
                c, s, f"{pin}{rng.randint(1, 99):03d}", reg)

    def _person(self, rng, *, last=None, gender=None, dob=None, address=None, mobile=None, first=None,
                relation="") -> Person:
        gender = gender or rng.choice("MF")
        first = first or rng.choice(FIRST_M if gender == "M" else FIRST_F)
        last = last or rng.choice(LAST)
        middle = rng.choice(FIRST_M) if rng.random() < 0.3 else ""
        dob = dob or date(1950, 1, 1) + timedelta(days=rng.randint(0, 20_000))
        line1, line2, city, state, pincode, region = address or self._address(rng)
        seg = rng.random()
        p = Person(len(self.persons) + 1, first, middle, last, gender, dob, self._new_pan(rng, last),
                   rng.choice("23456789") + f"{rng.randint(0, 10**11 - 1):011d}", mobile or self._new_mobile(rng),
                   f"{first}.{last}{rng.randint(1, 99)}@example.com".lower(), line1, line2, city, state, pincode,
                   region, "MASS" if seg < 0.75 else "AFFLUENT" if seg < 0.95 else "PRIVATE", relation)
        self.persons.append(p)
        return p

    def _addr_of(self, p: Person):
        return (p.line1, p.line2, p.city, p.state, p.pincode, p.region)

    def _build(self) -> None:
        rng = R(self.seed, "persons")
        for i in range(self.n):
            if i > 10 and rng.random() < 0.06:
                head = rng.choice(self.persons[:-1])
                gap = rng.choice([-1, 1]) * rng.randint(2, 30)
                dob = date(min(2004, max(1950, head.dob.year + gap)), rng.randint(1, 12), rng.randint(1, 28))
                self._person(rng, last=head.last, dob=dob, address=self._addr_of(head), mobile=head.mobile,
                             relation=f"household:{head.pid}")
            else:
                self._person(rng)
        for head in rng.sample(self.persons[:self.n], 2):    # same name and date of birth, different person
            self._person(rng, last=head.last, gender=head.gender, first=head.first, dob=head.dob,
                         relation=f"namesake:{head.pid}")
        presence = R(self.seed, "presence")
        self.late_cbs: list[Person] = []
        for p in self.persons:
            in_cbs = presence.random() < 0.88
            p.in_lms = presence.random() < (0.28 if in_cbs else 0.6)
            p.in_crm = presence.random() < (0.5 if in_cbs else 0.3)
            if not (in_cbs or p.in_lms or p.in_crm):
                in_cbs = True
            p.in_cbs = in_cbs
        for p in [q for q in self.persons if not q.in_cbs and q.in_lms][: max(2, self.n // 200)]:
            self.late_cbs.append(p)
        self._build_cbs()
        self._build_lms()
        self._build_crm()

    # ------------------------------------------------------------ cbs

    def _cbs_row(self, rng, p: Person, d_created: datetime, duplicate: bool = False) -> dict:
        cust_id = self._cust_seq
        self._cust_seq += 1
        first, middle, last = p.first.upper(), p.middle.upper() or None, p.last.upper()
        mobile, pan, addr1 = p.mobile, p.pan, p.line1
        email = p.email if rng.random() < 0.7 else f"{p.first}{p.last}@oldmail.example".lower()
        if duplicate:
            first = first[0] if rng.random() < 0.4 else first
            middle = middle[0] if middle and rng.random() < 0.5 else None
            last = typo(rng, last) if rng.random() < 0.2 else last
            mobile = fmt_mobile(rng, p.mobile)
            pan = None if rng.random() < 0.5 else p.pan
            addr1 = f"{rng.randint(1, 250)}, {rng.choice(STREETS)}" if rng.random() < 0.5 else p.line1
        kyc = rng.random()
        title = "DR" if rng.random() < 0.03 else "MR" if p.gender == "M" else rng.choice(["MRS", "MS"])
        row = {"cust_id": cust_id, "title": title, "first_name": first, "middle_name": middle, "last_name": last,
               "dob": p.dob.isoformat(), "gender": p.gender, "pan": pan, "aadhaar": p.aadhaar, "mobile": mobile,
               "email": email, "addr_line1": addr1, "addr_line2": p.line2, "city": p.city, "state": p.state,
               "pincode": p.pincode, "kyc_status": "VERIFIED" if kyc < 0.9 else "PENDING" if kyc < 0.96 else "EXPIRED",
               "segment": p.segment, "home_branch": self.branch_by_city[p.city][0],
               "created_at": sql_dt(d_created), "updated_at": sql_dt(d_created)}
        self.customers[cust_id] = row
        self.cust_pid[cust_id] = p.pid
        p.systems["cbs"].append(cust_id)
        return row

    def _account(self, rng, cust: dict, product: str, opened: date, updated: datetime, joint=None) -> dict:
        b = int(cust["home_branch"][2:]) - 100
        acct_no = f"{b:04d}{self._acct_seq + 1_000_000:010d}"
        self._acct_seq += 1
        ptype = product[:2]
        rate = RATE[product] + (round(rng.uniform(-0.25, 0.25), 2) if ptype == "TD" else 0)
        row = {"acct_no": acct_no, "cust_id": cust["cust_id"], "joint_cust_id": joint, "product_code": product,
               "branch_code": cust["home_branch"], "currency": "INR", "open_date": opened.isoformat(),
               "status": "ACTIVE" if rng.random() < 0.95 else "DORMANT", "close_date": None,
               "interest_rate": round(rate, 2), "updated_at": sql_dt(updated)}
        scale = {"MASS": 1, "AFFLUENT": 4, "PRIVATE": 15}[cust["segment"]]
        if ptype == "SA":
            bal = rng.lognormvariate(10.4, 1.0) * scale
        elif ptype == "CA":
            bal = rng.lognormvariate(11.8, 1.0) * scale
        else:
            bal = round(rng.lognormvariate(12.2, 0.8) * scale, -3)
        self.accounts[acct_no] = row
        self.balances[acct_no] = round(bal, 2)
        return row

    def _open_accounts(self, rng, cust: dict, opened: date, updated: datetime, minimal: bool = False) -> list:
        made = []
        if minimal or rng.random() < 0.85:
            made.append(self._account(rng, cust, "SA02" if rng.random() < 0.3 else "SA01", opened, updated))
        if not minimal and rng.random() < (0.3 if cust["segment"] != "MASS" else 0.08):
            made.append(self._account(rng, cust, "CA01", opened, updated))
        if not minimal and rng.random() < 0.35:
            for _ in range(rng.choice([1, 1, 2])):
                made.append(self._account(rng, cust, rng.choice(["TD01", "TD02"]), opened, updated))
        if not made:
            made.append(self._account(rng, cust, "SA01", opened, updated))
        return made

    def _build_cbs(self) -> None:
        rng = R(self.seed, "cbs")
        late = {p.pid for p in self.late_cbs}
        for p in self.persons:
            if not p.in_cbs or p.pid in late:
                continue
            created = datetime(2010, 1, 1) + timedelta(days=rng.randint(0, 6000), seconds=rng.randint(0, 86399))
            cust = self._cbs_row(rng, p, created)
            self._open_accounts(rng, cust, created.date(), created)
            if rng.random() < 0.05:   # a second CIF opened later, for the same person
                created2 = min(created + timedelta(days=rng.randint(200, 2000)), datetime(2026, 8, 31))
                dup = self._cbs_row(rng, p, created2, duplicate=True)
                self._open_accounts(rng, dup, created2.date(), created2, minimal=True)
        by_pid = {pid: cid for cid, pid in self.cust_pid.items()}
        for acct in self.accounts.values():   # joint holders from the same household
            p = self.persons[self.cust_pid[acct["cust_id"]] - 1]
            if acct["product_code"].startswith("SA") and p.relation.startswith("household") and rng.random() < 0.5:
                acct["joint_cust_id"] = by_pid.get(int(p.relation.split(":")[1]))
        rows = list(self.customers.values())
        faults = rng.sample(rows, 7)
        self.dump_faults = {"missing_last_name": [], "impossible_dob": [], "bad_pan": []}
        for r in faults[:3]:
            r["last_name"] = ""
            self.dump_faults["missing_last_name"].append(r["cust_id"])
        for r in faults[3:5]:
            r["dob"] = "1985-02-30"
            self.dump_faults["impossible_dob"].append(r["cust_id"])
        for r in faults[5:]:
            r["pan"] = r["pan"][:8] if r["pan"] else "ABCD1234"
            self.dump_faults["bad_pan"].append(r["cust_id"])

    # ------------------------------------------------------------ lms

    def _borrower(self, rng, p: Person, created: date, variant: bool = False) -> dict:
        bid = f"LB{self._borr_seq:06d}"
        self._borr_seq += 1
        style = rng.random()
        first, last = (p.first, p.last) if not variant else (typo(rng, p.first), p.last)
        if style < 0.6:
            name = " ".join(x for x in (first, p.middle, last) if x)
        elif style < 0.8:
            name = f"{last.upper()} {first.upper()}"
        elif style < 0.9:
            name = f"{first[0]}. {last}"
        else:
            name = f"{first} {typo(rng, last)}"
        pan = p.pan if rng.random() < 0.85 else ""
        if pan and rng.random() < 0.1:
            pan = pan.lower()
        line1 = p.line1 if rng.random() < 0.7 else f"{rng.randint(1, 250)}, {rng.choice(STREETS)}"
        address = f"{abbreviate(line1) if rng.random() < 0.5 else line1}, {p.line2}"
        cbs = p.systems["cbs"][0] if p.systems["cbs"] and rng.random() < 0.5 else ""
        updated = created + timedelta(days=rng.randint(0, 400))
        row = {"borrower_id": bid, "full_name": name, "dob": dmy(p.dob), "pan": pan,
               "mobile": fmt_mobile(rng, p.mobile), "address": address, "city": p.city, "pincode": p.pincode,
               "cbs_cust_ref": str(cbs), "created_on": dmy(created), "updated_on": dmy(min(updated, DATES[0]))}
        self.borrowers[bid] = row
        p.systems["lms"].append(bid)
        return row

    def _loan(self, rng, borrower: dict, p: Person, sanction: date | None = None, state: str | None = None) -> dict:
        lid = f"LN{self._loan_seq + 10_000_000:08d}"
        self._loan_seq += 1
        u = rng.random()
        product = "HL01" if u < 0.3 else "PL01" if u < 0.65 else "AL01" if u < 0.85 else "BL01"
        lo, hi, tenure = LOAN_SPEC[product]
        amount = round(rng.uniform(lo, hi), -3)
        rate = round(RATE[product] + rng.uniform(-1, 1), 2)
        if sanction is None:
            sanction = add_months(DATES[0], -rng.randint(3, min(tenure - 2, 96)))
        elapsed = months_between(sanction, DATES[0])
        e = emi(amount, rate, tenure)
        if state is None:
            s = rng.random()
            state = ("REGULAR" if s < 0.84 else "SMA" if s < 0.90 else "SUB" if s < 0.96
                     else "DOUBTFUL" if s < 0.985 else "LOSS")
        dpd = {"REGULAR": 0, "SMA": rng.randint(1, 84), "SUB": rng.randint(91, 455),
               "DOUBTFUL": rng.randint(456, 1500), "LOSS": rng.randint(456, 2000), "NEW": 0}[state]
        dpd = min(dpd, max(0, (DATES[0] - sanction).days - 30)) if state != "NEW" else 0
        missed = math.ceil(dpd / 30)
        outstanding = balance_after(amount, rate, tenure, max(0, elapsed - missed)) if state != "NEW" else amount
        last_paid = DATES[0] - timedelta(days=dpd + rng.randint(0, 10)) if dpd else DATES[0] - timedelta(
            days=rng.randint(1, 30))
        row = {"loan_id": lid, "borrower_id": borrower["borrower_id"], "product_code": product,
               "branch_code": self.branch_by_city[p.city][0], "sanction_date": sanction, "sanction_amount": amount,
               "interest_rate": rate, "tenure_months": tenure, "emi_amount": round(e, 2),
               "principal_outstanding": round(outstanding, 2), "principal_overdue": round(e * missed * 0.7, 2),
               "interest_overdue": round(e * missed * 0.3, 2), "dpd": dpd,
               "last_payment_date": max(last_paid, sanction), "loan_status": "ACTIVE",
               "restructured_flag": "Y" if rng.random() < 0.03 else "N", "loss_flag": "Y" if state == "LOSS" else "N"}
        self.loans[lid] = row
        return row

    def _build_lms(self) -> None:
        rng = R(self.seed, "lms")
        for p in self.persons:
            if not p.in_lms:
                continue
            created = DATES[0] - timedelta(days=rng.randint(100, 2800))
            b = self._borrower(rng, p, created)
            self._loan(rng, b, p)
            if rng.random() < 0.2:
                if rng.random() < 0.3:   # applied again, keyed as a new borrower
                    b = self._borrower(rng, p, created + timedelta(days=rng.randint(30, 90)), variant=True)
                self._loan(rng, b, p)
        regular = [loan for loan in self.loans.values() if loan["dpd"] == 0]
        for dpd, loan in zip((87, 88, 89), rng.sample(regular, 3)):   # slip into NPA on dates 5, 4, 3
            missed = math.ceil(dpd / 30)
            loan.update(dpd=dpd, principal_overdue=round(loan["emi_amount"] * missed * 0.7, 2),
                        interest_overdue=round(loan["emi_amount"] * missed * 0.3, 2),
                        last_payment_date=DATES[0] - timedelta(days=dpd))
        self.slipping = [loan["loan_id"] for loan in self.loans.values() if loan["dpd"] in (87, 88, 89)][:3]
        subs = [loan for loan in self.loans.values() if 91 <= loan["dpd"] <= 455]
        self.upgrade = rng.choice(subs)["loan_id"] if subs else None
        self.prepay = rng.choice([loan for loan in regular if loan["loan_id"] not in self.slipping])["loan_id"]
        self.src_lag = [loan["loan_id"] for loan in rng.sample(subs, min(2, len(subs)))]

    @staticmethod
    def _src_class(loan: dict, lagging: bool) -> str:
        dpd = loan["dpd"]
        if loan["loan_status"] == "CLOSED":
            return "CLOSED"
        if dpd <= 90 or lagging:
            return "STANDARD"
        return "LOSS" if loan["loss_flag"] == "Y" else "SUBSTANDARD" if dpd <= 455 else "DOUBTFUL"

    # ------------------------------------------------------------ crm

    def _crm_doc(self, rng, p: Person, updated: datetime) -> dict:
        crm_id = f"CRM-{rng.getrandbits(32):08x}"
        style = rng.random()
        name = f"{p.first} {p.last}" if style < 0.75 else f"Mr. {p.first} {p.last}" if p.gender == "M" and \
            style < 0.85 else f"{p.first} {typo(rng, p.last)}"
        dob = p.dob.isoformat() if rng.random() < 0.8 else p.dob.strftime("%d-%m-%Y")
        ids = [{"type": "MOBILE", "value": fmt_mobile(rng, p.mobile)}]
        if rng.random() < 0.7:
            ids.insert(0, {"type": "PAN", "value": p.pan})
        doc = {"crm_id": crm_id, "source_channel": rng.choice(["MOBILE_APP", "WEB", "BRANCH"]),
               "profile": {"full_name": name, "date_of_birth": dob,
                           "gender": "male" if p.gender == "M" else "female"},
               "identifiers": ids,
               "addresses": [{"type": "HOME", "line1": p.line1, "line2": p.line2, "city": p.city,
                              "pincode": p.pincode, "updated_on": (updated.date() - timedelta(
                                  days=rng.randint(0, 300))).isoformat()}],
               "contacts": {"email": p.email, "mobile": p.mobile},
               "consent": {"marketing": rng.random() < 0.55, "updated_on": updated.date().isoformat()},
               "updated_at": updated.strftime("%Y-%m-%dT%H:%M:%S+05:30")}
        self.crm[crm_id] = doc
        p.systems["crm"].append(crm_id)
        return doc

    def _build_crm(self) -> None:
        rng = R(self.seed, "crm")
        for p in self.persons:
            if p.in_crm:
                self._crm_doc(rng, p, datetime(2026, 9, 20, 12) - timedelta(days=rng.randint(0, 700)))

    # ------------------------------------------------------------ one business date

    def _day(self, d: date, prev: date | None) -> dict:
        rng = R(self.seed, f"day:{d}")
        out = {"cdc": [], "eod": [], "borrowers": [], "loans": [], "repayments": [], "payments": [], "crm": [],
               "docs": [], "dump": None, "faults": {}}
        idx = DATES.index(d)
        if prev is None:
            out["dump"] = {"branch": [dict(zip([c for c, _ in CBS_TABLES["branch"]], b[:6] + (b[6].isoformat(),)))
                                      for b in self.branches],
                           "product": [dict(zip([c for c, _ in CBS_TABLES["product"]], p)) for p in PRODUCTS],
                           "customer": [dict(r) for r in self.customers.values()],
                           "account": [dict(r) for r in self.accounts.values()]}
            out["crm"] = [json.loads(json.dumps(doc)) for doc in self.crm.values()]
            self._fix_dump_faults()
            out["faults"]["dump"] = self.dump_faults
        else:
            out["cdc"], changes = self._cdc(rng, d, idx)
            out["crm"] = self._crm_changes(rng, d, changes)
            out["faults"]["cdc"] = changes["faults"]
        out["eod"] = self._eod(rng, d, idx)
        out["borrowers"], out["loans"], out["repayments"] = self._lms_day(rng, d, prev, idx)
        out["payments"] = self._payments(rng, d, idx)
        out["docs"] = self._documents(rng, d, idx, out)
        return out

    def _fix_dump_faults(self) -> None:
        """After the dump: the source fixes a missing last name on the next date (a CDC update); impossible
        dates of birth and bad PANs stay as they are in the system."""
        self._pending_fix = self.dump_faults["missing_last_name"][0]

    def _cdc_event(self, op, table, before, after, ts: datetime, pos: list) -> dict:
        pos[0] += rng_step(pos)
        return {"op": op, "ts_ms": epoch_ms(ts), "source": {"connector": "mysql", "db": "cbs", "table": table,
                                                         "file": pos[1], "pos": pos[0], "snapshot": False},
                "before": dict(before) if before else None, "after": dict(after) if after else None}

    def _cdc(self, rng, d: date, idx: int):
        events, pos = [], [4, f"mysql-bin.{100 + idx:06d}"]
        changes = {"address": [], "kyc_verified": [], "new_customers": [], "faults": {}}
        live = [c for c in self.customers.values() if c["last_name"] != "" or c["cust_id"] == self._pending_fix]

        def update(table, row, **kw):
            before = dict(row)
            ts = ist(d, rng)
            row.update(kw, updated_at=sql_dt(ts))
            events.append(self._cdc_event("u", table, before, row, ts, pos))
            return ts

        if idx == 1 and self._pending_fix in self.customers:
            p = self.persons[self.cust_pid[self._pending_fix] - 1]
            update("customer", self.customers[self._pending_fix], last_name=p.last.upper())
            changes["faults"]["corrected_after_reject"] = self._pending_fix
        for c in rng.sample(live, max(1, round(0.012 * len(live)))):
            p = self.persons[self.cust_pid[c["cust_id"]] - 1]
            line1, line2, city, state, pin, _ = self._address(rng, next(x for x in CITIES if x[0] == c["city"])
                                                              if rng.random() < 0.8 else None)
            ts = update("customer", c, addr_line1=line1, addr_line2=line2, city=city, state=state, pincode=pin)
            p.line1, p.line2, p.city, p.state, p.pincode = line1, line2, city, state, pin
            changes["address"].append((c["cust_id"], ts))
        for c in rng.sample(live, max(1, round(0.006 * len(live)))):
            p = self.persons[self.cust_pid[c["cust_id"]] - 1]
            p.mobile = self._new_mobile(rng)
            update("customer", c, mobile=p.mobile)
        for c in rng.sample(live, max(1, round(0.005 * len(live)))):
            if c["kyc_status"] != "VERIFIED":
                update("customer", c, kyc_status="VERIFIED")
                changes["kyc_verified"].append(c["cust_id"])
            else:
                update("customer", c, kyc_status="EXPIRED")
        for c in rng.sample(live, max(1, round(0.003 * len(live)))):
            if c["segment"] == "MASS":
                update("customer", c, segment="AFFLUENT")
        joiners = self.late_cbs[idx - 1::len(DATES) - 1]
        joiners += [self._person(rng) for _ in range(max(1, round(0.002 * self.n)))]
        for p in joiners:
            ts = ist(d, rng)
            cust = self._cbs_row(rng, p, ts)
            events.append(self._cdc_event("c", "customer", None, cust, ts, pos))
            for a in self._open_accounts(rng, cust, d, ts, minimal=True):
                events.append(self._cdc_event("c", "account", None, a, ts + timedelta(seconds=5), pos))
            changes["new_customers"].append(cust["cust_id"])
        active = [a for a in self.accounts.values() if a["status"] != "CLOSED"]
        for a in rng.sample(active, max(1, round(0.003 * len(active)))):
            update("account", a, status="DORMANT")
        for a in rng.sample(active, max(1, round(0.002 * len(active)))):
            update("account", a, status="CLOSED", close_date=d.isoformat())
        for a in rng.sample(active, max(1, round(0.002 * len(active)))):
            update("account", a, branch_code=rng.choice([b[0] for b in self.branches if b[0] != a["branch_code"]]))
        events.sort(key=lambda e: e["ts_ms"])
        if idx == 2:   # the second change of one customer arrives before the first
            c = self.customers[changes["address"][0][0]]
            first_i = next(i for i, e in enumerate(events) if e["source"]["table"] == "customer"
                           and e["after"] and e["after"]["cust_id"] == c["cust_id"])
            ts = datetime(d.year, d.month, d.day, 18, 30)
            before = dict(c)
            c.update(email=f"{c['first_name']}.{c['cust_id']}@example.com".lower(), updated_at=sql_dt(ts))
            events.insert(first_i, self._cdc_event("u", "customer", before, c, ts, pos))
            changes["faults"]["out_of_order"] = c["cust_id"]
        if idx == 3:
            dups = [cid for p in self.persons for cid in p.systems["cbs"][1:]]
            if dups:
                gone = self.customers.pop(dups[0])
                self.persons[self.cust_pid[gone["cust_id"]] - 1].systems["cbs"].remove(gone["cust_id"])
                ts = ist(d, rng)
                for a in [a for a in self.accounts.values() if a["cust_id"] == gone["cust_id"]]:
                    update("account", a, status="CLOSED", close_date=d.isoformat())
                events.append(self._cdc_event("d", "customer", gone, None, ts, pos))
                changes["faults"]["deleted_duplicate_cif"] = gone["cust_id"]
            ghost = {c: None for c, _ in CBS_TABLES["customer"]}
            ghost.update(cust_id=999999, last_name="UNKNOWN", first_name="GHOST")
            events.append(self._cdc_event("d", "customer", ghost, None, ist(d, rng), pos))
            changes["faults"]["delete_unknown_key"] = 999999
        if idx == 4:
            events.append(json.loads(json.dumps(events[len(events) // 2])))
            changes["faults"]["resent_event_pos"] = events[-1]["source"]["pos"]
        return events, changes

    def _crm_changes(self, rng, d: date, changes: dict) -> list:
        docs = []
        pid_crm = {cid: p for p in self.persons for cid in p.systems["crm"]}
        for crm_id in rng.sample(list(self.crm), max(1, round(0.01 * len(self.crm)))):
            p = pid_crm[crm_id]
            doc = self.crm[crm_id]
            p.email = f"{p.first}.{p.last}.{d.day}{rng.randint(1, 9)}@example.com".lower()
            doc["contacts"]["email"] = p.email
            doc["updated_at"] = ist(d, rng).strftime("%Y-%m-%dT%H:%M:%S+05:30")
            docs.append(doc)
        for cust_id, ts in changes["address"]:
            p = self.persons[self.cust_pid[cust_id] - 1]
            if p.systems["crm"] and rng.random() < 0.3:
                doc = self.crm[p.systems["crm"][0]]
                doc["addresses"].insert(0, {"type": "HOME", "line1": p.line1, "line2": p.line2, "city": p.city,
                                            "pincode": p.pincode, "updated_on": d.isoformat()})
                doc["updated_at"] = (ts + timedelta(minutes=20)).strftime("%Y-%m-%dT%H:%M:%S+05:30")
                docs.append(doc)
        fresh = [p for p in self.persons if not p.systems["crm"] and p.systems["cbs"]]
        for p in rng.sample(fresh, min(len(fresh), max(1, round(0.003 * self.n)))):
            docs.append(self._crm_doc(rng, p, ist(d, rng)))
        unique = {doc["crm_id"]: json.loads(json.dumps(doc)) for doc in docs}
        return list(unique.values())

    def _eod(self, rng, d: date, idx: int) -> list:
        rows = []
        for acct_no, a in self.accounts.items():
            if a["status"] == "CLOSED" and a["close_date"] != d.isoformat():
                continue
            if a["status"] == "CLOSED":
                self.balances[acct_no] = 0.0
            elif not a["product_code"].startswith("TD") and idx:
                self.balances[acct_no] = round(max(0.0, self.balances[acct_no] * (1 + rng.gauss(0, 0.03))), 2)
            bal = self.balances[acct_no]
            hold = round(bal * 0.02, 2) if rng.random() < 0.03 else 0.0
            rows.append({"acct_no": acct_no, "bal_date": d.isoformat(), "ledger_balance": f"{bal:.2f}",
                         "available_balance": f"{bal - hold:.2f}", "currency": a["currency"]})
        if idx == 1:
            rows[7]["ledger_balance"] = "N/A"
        if idx == 3:
            rows.insert(12, dict(rows[11]))
        return rows

    def _lms_day(self, rng, d: date, prev: date | None, idx: int):
        repayments = []
        if prev is not None:
            gap = (d - prev).days
            for lid, loan in self.loans.items():
                if loan["loan_status"] == "CLOSED":
                    continue
                if loan["dpd"] == 0:
                    if lid == self.prepay and idx == 3:
                        repayments.append((lid, loan["principal_outstanding"], "PREPAYMENT"))
                        loan.update(principal_outstanding=0.0, loan_status="CLOSED", last_payment_date=d)
                    elif rng.random() < 0.04:
                        r = loan["interest_rate"] / 1200
                        principal = loan["emi_amount"] - loan["principal_outstanding"] * r
                        loan["principal_outstanding"] = round(max(0.0, loan["principal_outstanding"] - principal), 2)
                        loan["last_payment_date"] = d
                        repayments.append((lid, loan["emi_amount"], "NACH"))
                    continue
                cure = (lid == self.upgrade and idx == 4) or (
                    lid not in self.slipping and lid != self.upgrade and rng.random() < 0.03)
                if cure:
                    repayments.append((lid, round(loan["principal_overdue"] + loan["interest_overdue"], 2),
                                       "BRANCH_CASH"))
                    loan.update(dpd=0, principal_overdue=0.0, interest_overdue=0.0, last_payment_date=d)
                else:
                    loan["dpd"] += gap
                    loan["principal_overdue"] = round(loan["principal_overdue"] + loan["emi_amount"] * gap / 30 * 0.7, 2)
                    loan["interest_overdue"] = round(loan["interest_overdue"] + loan["emi_amount"] * gap / 30 * 0.3, 2)
            if idx == 2:   # an existing CBS customer takes a new personal loan, keyed as a new borrower
                p = next(q for q in self.persons if q.systems["cbs"] and not q.systems["lms"] and not q.relation)
                b = self._borrower(rng, p, d)
                b["pan"] = p.pan
                self._loan(rng, b, p, sanction=d, state="NEW")
                self.new_loan_person = p.pid
        borrowers = [dict(b) for b in self.borrowers.values()]
        loans = []
        for lid, loan in self.loans.items():
            row = {k: loan[k] for k in LOAN_COLS if k in loan}
            row.update(sanction_date=dmy(loan["sanction_date"]), last_payment_date=dmy(loan["last_payment_date"]),
                       as_of_date=dmy(d), src_asset_class=self._src_class(loan, lid in self.src_lag))
            loans.append({k: (f"{v:.2f}" if isinstance(v, float) else str(v)) for k, v in row.items()})
        if idx == 1:
            loans[5]["dpd"] = "abc"
        rep = [{"txn_id": f"RP{d:%Y%m%d}{i:05d}", "loan_id": lid, "payment_date": dmy(d), "amount": f"{amt:.2f}",
                "mode": mode} for i, (lid, amt, mode) in enumerate(repayments, 1)]
        return borrowers, loans, rep

    def _payments(self, rng, d: date, idx: int) -> list:
        accts = [a for a in self.accounts.values() if a["status"] == "ACTIVE" and a["product_code"][:2] in ("SA", "CA")]
        out, seq = [], [0]
        names = [f"{rng.choice(FIRST_M + FIRST_F)} {rng.choice(LAST)}" for _ in range(200)]

        def owner_name(a):
            c = self.customers.get(a["cust_id"])
            return f"{c['first_name']} {c['last_name']}".strip() if c else "UNKNOWN"

        def payment(a, direction, channel, amount, ccy="INR", counterparty=None, when=None):
            seq[0] += 1
            when = when or ist(d, rng, 0, 23)
            ours = {"name": owner_name(a), "account": {"number": a["acct_no"],
                                                        "ifsc": next(b[1] for b in self.branches
                                                                     if b[0] == a["branch_code"])}}
            if counterparty is None:
                bank = rng.choice(OTHER_BANKS) if rng.random() < 0.8 else BANK_IFSC
                counterparty = {"name": rng.choice(names),
                                "account": {"number": f"{rng.randint(10**11, 10**12 - 1)}",
                                            "ifsc": f"{bank}0{rng.randint(1, 999999):06d}"}}
            fee = CHANNEL_FEE.get(channel, 0.0) if direction == "DR" or channel == "SWIFT" else 0.0
            status = "SETTLED" if rng.random() < 0.97 else rng.choice(["REJECTED", "RETURNED"])
            doc = {"msg_id": f"PMT{d:%Y%m%d}{seq[0]:06d}", "end_to_end_id": f"E2E{rng.getrandbits(48):012x}",
                   "created_at": when.strftime("%Y-%m-%dT%H:%M:%S+05:30"), "value_date": d.isoformat(),
                   "channel": channel, "direction": direction, "status": status,
                   "amount": {"value": f"{amount:.2f}", "ccy": ccy},
                   "debtor": ours if direction == "DR" else counterparty,
                   "creditor": counterparty if direction == "DR" else ours,
                   "remittance": {"purpose": rng.choice(["P0101", "P0103", "P1006", "P0802"]),
                                  "narrative": rng.choice(["rent", "salary", "invoice", "fees", "gift", "emi",
                                                           "groceries", "transfer"])},
                   "charges": ([{"type": "TXN_FEE", "amount": f"{fee:.2f}", "ccy": "INR"},
                                {"type": "GST", "amount": f"{fee * 0.18:.2f}", "ccy": "INR"}] if fee else [])}
            if idx >= 3 and channel in ("UPI", "IMPS", "CARD"):
                doc["device"] = {"id": f"dv-{rng.getrandbits(32):08x}", "os": rng.choice(["android", "ios"]),
                                 "ip": f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"}
            out.append(doc)
            return doc

        channels = [("UPI", 0.5), ("IMPS", 0.15), ("NEFT", 0.2), ("RTGS", 0.03), ("CARD", 0.1), ("CASH_DEPOSIT", 0.015),
                    ("SWIFT", 0.005)]
        for _ in range(int(1.5 * self.n)):
            a = rng.choice(accts)
            u, acc = rng.random(), 0.0
            channel = channels[-1][0]
            for ch, w in channels:
                acc += w
                if u < acc:
                    channel = ch
                    break
            if channel == "SWIFT":
                payment(a, "CR", "SWIFT", rng.uniform(500, 5000), rng.choice(["USD", "EUR", "GBP"]))
                continue
            direction = "CR" if channel == "CASH_DEPOSIT" else rng.choice(["DR", "CR"])
            amount = {"UPI": rng.lognormvariate(6.5, 1.0), "IMPS": rng.lognormvariate(8.5, 1.0),
                      "NEFT": rng.lognormvariate(9.5, 1.0), "RTGS": rng.uniform(200_000, 2_500_000),
                      "CARD": rng.lognormvariate(7.5, 0.9), "CASH_DEPOSIT": rng.lognormvariate(9.0, 0.8)}[channel]
            payment(a, direction, channel, amount)
        aml_rng = R(self.seed, "aml")
        planted = aml_rng.sample(accts, 4)
        if idx in (1, 2, 3):   # structuring: cash deposits just under 50,000 on three days
            for a in planted[:3]:
                for _ in range(aml_rng.choice([1, 2])):
                    payment(a, "CR", "CASH_DEPOSIT", aml_rng.uniform(45_000, 49_900))
        if idx == 4:           # pass-through: many small credits, one large debit out
            a, total = planted[3], 0.0
            for i in range(12):
                amt = aml_rng.uniform(5_000, 9_000)
                total += amt
                payment(a, "CR", "UPI", amt, when=datetime(d.year, d.month, d.day, 10, 5 * i))
            payment(a, "DR", "RTGS", total * 0.95, when=datetime(d.year, d.month, d.day, 15, 0))
        self.aml_planted = [a["acct_no"] for a in planted]
        out[3]["amount"] = {"ccy": "INR"}
        out[40]["amount"] = {"ccy": "INR"}
        out.insert(60, json.loads(json.dumps(out[59])))
        if idx == 1:
            out[80]["amount"]["ccy"] = "XYZ"
        return out

    def _documents(self, rng, d: date, idx: int, day: dict) -> list:
        docs, seq = [], [0]

        def name(kind, ext):
            seq[0] += 1
            return f"{kind}-{d:%Y%m%d}-{seq[0]:04d}.{ext}"

        def eml(p: Person, subject: str, body: str) -> bytes:
            when = ist(d, rng)
            return (f'From: "{p.first} {p.last}" <{p.email}>\nTo: customer.care@gdlbank.example\n'
                    f"Date: {when.strftime('%a, %d %b %Y %H:%M:%S')} +0530\nSubject: {subject}\n"
                    f"Message-ID: <{rng.getrandbits(64):016x}@example.com>\nContent-Type: text/plain; charset=utf-8\n\n"
                    f"{body}\n").encode()

        accounts_of = {}
        for a in self.accounts.values():
            accounts_of.setdefault(a["cust_id"], []).append(a["acct_no"])
        for cust_id, _ in [x for x in self._cdc_changes(day) if rng.random() < 0.5]:
            p = self.persons[self.cust_pid[cust_id] - 1]
            acct = accounts_of.get(cust_id, ["(not known)"])[0]
            pan = f"My PAN is {p.pan}.\n" if rng.random() < 0.7 else ""
            body = (f"Dear Sir/Madam,\n\nPlease update my communication address on account {acct} to:\n"
                    f"{p.line1}, {p.line2}, {p.city} - {p.pincode}.\n{pan}My registered mobile is {p.mobile}.\n\n"
                    f"Regards,\n{p.first} {p.last}")
            docs.append((name("EML", "eml"), eml(p, "Request to update communication address", body)))
        holders = [c for c in self.customers.values() if c["cust_id"] in accounts_of]
        for c in rng.sample(holders, max(1, round(0.002 * self.n))):
            p = self.persons[self.cust_pid[c["cust_id"]] - 1]
            body = (f"Hello,\n\nI was charged twice for an NEFT transfer from account {accounts_of[c['cust_id']][0]} "
                    f"on {d.strftime('%d %b %Y')}. Please reverse the duplicate charge.\n\nThanks,\n{p.first} {p.last}")
            docs.append((name("EML", "eml"), eml(p, "Complaint: duplicate charge", body)))
        if idx == 0:
            kyc = [c["cust_id"] for c in rng.sample(list(self.customers.values()), max(1, round(0.005 * self.n)))]
        else:
            kyc = self._kyc_changes(day)
        for cust_id in kyc:
            p = self.persons[self.cust_pid[cust_id] - 1]
            text = (f"KYC DECLARATION\nCustomer ID: {cust_id}\nName: {p.first} {p.middle + ' ' if p.middle else ''}"
                    f"{p.last}\nDate of birth: {p.dob.strftime('%d/%m/%Y')}\nPAN: {p.pan}\n"
                    f"Aadhaar: XXXX XXXX {p.aadhaar[-4:]}\nAddress: {p.line1}, {p.line2}, {p.city} {p.pincode}\n"
                    f"Mobile: {p.mobile}\nI declare that the above information is true.\nSigned on {d.isoformat()}\n")
            stem = name("KYC", "txt")
            docs.append((stem, text.encode()))
            docs.append((stem.replace("KYC-", "SCAN-").replace(".txt", ".png"), tiny_png(rng)))
        if idx == 2:
            docs.append((name("EML", "eml"), b""))
        return docs

    @staticmethod
    def _cdc_changes(day: dict) -> list:
        out = []
        for e in day["cdc"]:
            a, b = e.get("after"), e.get("before")
            if e["op"] == "u" and e["source"]["table"] == "customer" and a and b and a["addr_line1"] != b["addr_line1"]:
                out.append((a["cust_id"], e["ts_ms"]))
        return out

    @staticmethod
    def _kyc_changes(day: dict) -> list:
        return [e["after"]["cust_id"] for e in day["cdc"] if e["op"] == "u" and e["source"]["table"] == "customer"
                and e["after"]["kyc_status"] == "VERIFIED" and e["before"]["kyc_status"] != "VERIFIED"]

    def truth(self) -> dict:
        return {"persons": [{"pid": p.pid, "relation": p.relation, "cbs": p.systems["cbs"], "lms": p.systems["lms"],
                             "crm": p.systems["crm"]} for p in self.persons],
                "slipping_loans": self.slipping, "upgrade_loan": self.upgrade, "prepaid_loan": self.prepay,
                "source_class_lag": self.src_lag, "aml_planted_accounts": self.aml_planted,
                "faults": {d.isoformat(): self.out[d]["faults"] for d in DATES}}


def rng_step(pos: list) -> int:
    return 180 + (pos[0] * 7919) % 211


def tiny_png(rng: random.Random, w: int = 48, h: int = 30) -> bytes:
    raw = b"".join(b"\x00" + bytes(rng.randint(150, 255) for _ in range(w)) for _ in range(h))

    def chunk(tag: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)

    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 0, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b""))


# ---------------------------------------------------------------- file formats


def sql_value(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def mysqldump(tables: dict, d: date) -> bytes:
    lines = ["-- MySQL dump 10.13  Distrib 8.0.36, for Linux (x86_64)", "--",
             "-- Host: cbs-db.internal    Database: cbs",
             "-- ------------------------------------------------------", "-- Server version\t8.0.36", "",
             "/*!40101 SET NAMES utf8mb4 */;", "/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, "
             "FOREIGN_KEY_CHECKS=0 */;", ""]
    for table, rows in tables.items():
        cols = CBS_TABLES[table]
        lines += [f"--\n-- Table structure for table `{table}`\n--", "", f"DROP TABLE IF EXISTS `{table}`;",
                  f"CREATE TABLE `{table}` ("]
        lines += [f"  `{c}` {t}{' NOT NULL' if i == 0 else ' DEFAULT NULL'}," for i, (c, t) in enumerate(cols)]
        lines += [f"  PRIMARY KEY (`{cols[0][0]}`)", ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", "",
                  f"--\n-- Dumping data for table `{table}`\n--", "", f"LOCK TABLES `{table}` WRITE;"]
        for i in range(0, len(rows), 100):
            values = ",".join("(" + ",".join(sql_value(r[c]) for c, _ in cols) + ")" for r in rows[i:i + 100])
            lines.append(f"INSERT INTO `{table}` VALUES {values};")
        lines += ["UNLOCK TABLES;", ""]
    lines.append(f"-- Dump completed on {d.isoformat()} 23:55:01")
    return ("\n".join(lines) + "\n").encode()


def psv(cols: list, rows: list, control_field: str | None, trailer_extra: int = 0) -> tuple[bytes, float | None]:
    total = None
    if control_field:
        total = round(sum(_num(r[control_field]) for r in rows), 2)
    body = ["|".join(cols)] + ["|".join("" if r[c] is None else str(r[c]) for c in cols) for r in rows]
    body.append(f"T|{len(rows) + trailer_extra}|{total if total is not None else ''}")
    return ("\n".join(body) + "\n").encode(), total


def _num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def jsonl(docs: list) -> bytes:
    return ("\n".join(json.dumps(doc, separators=(",", ":")) for doc in docs) + "\n").encode()


def day_files(world: World, d: date) -> dict:
    """source -> (files {name: bytes}, entities [{entity, file, records, control_field, control_total}])"""
    day, ymd = world.out[d], f"{d:%Y%m%d}"
    out = {}
    files, ents = {}, []
    if day["dump"]:
        name = f"cbs_dump_{ymd}.sql"
        files[name] = mysqldump(day["dump"], d)
        for table, rows in day["dump"].items():
            ents.append({"entity": f"cbs_{table}", "file": name, "records": len(rows)})
    else:
        name = f"cbs_cdc_{ymd}.jsonl"
        files[name] = jsonl(day["cdc"])
        ents.append({"entity": "cbs_cdc_event", "file": name, "records": len(day["cdc"])})
    name = f"cbs_eod_balance_{ymd}.csv"
    files[name], total = psv(EOD_COLS, day["eod"], "ledger_balance")
    ents.append({"entity": "cbs_eod_balance", "file": name, "records": len(day["eod"]),
                 "control_field": "ledger_balance", "control_total": total})
    out["cbs"] = (files, ents)

    files, ents = {}, []
    idx = DATES.index(d)
    for entity, cols, rows, field_ in (("lms_borrower", BORROWER_COLS, day["borrowers"], None),
                                       ("lms_loan", LOAN_COLS, day["loans"], "principal_outstanding"),
                                       ("lms_repayment", REPAYMENT_COLS, day["repayments"], "amount")):
        name = f"{entity}_{ymd}.csv"
        files[name], total = psv(cols, rows, field_, trailer_extra=1 if entity == "lms_repayment" and idx == 2 else 0)
        ents.append({"entity": entity, "file": name, "records": len(rows), "control_field": field_,
                     "control_total": total})
    out["lms"] = (files, ents)

    name = f"payments_{ymd}.jsonl"
    total = round(sum(_num(p["amount"].get("value")) for p in day["payments"]), 2)
    out["payments"] = ({name: jsonl(day["payments"])},
                       [{"entity": "pay_transaction", "file": name, "records": len(day["payments"]),
                         "control_field": "amount.value", "control_total": total}])
    name = f"crm_customers_{ymd}.json"
    out["crm"] = ({name: json.dumps(day["crm"], indent=1).encode()},
                  [{"entity": "crm_customer", "file": name, "records": len(day["crm"])}])
    docs = dict(day["docs"])
    out["documents"] = (docs, [{"entity": "doc_document", "file": "*", "records": len(docs)}])
    return out


def manifest(source: str, d: date, files: dict, ents: list) -> bytes:
    return json.dumps({
        "source": source, "business_date": d.isoformat(), "batch_id": C.batch_id(d),
        "generated_at": f"{d.isoformat()}T23:58:00+05:30", "entities": ents,
        "files": [{"file": n, "bytes": len(b), "sha256": hashlib.sha256(b).hexdigest()} for n, b in sorted(files.items())],
    }, indent=1).encode()


def land(world: World, d: date, landing: str, fs) -> dict:
    """Write one business date's files and manifests; returns {source: number of files}."""
    written = {}
    for source, (files, ents) in day_files(world, d).items():
        folder = f"{landing.rstrip('/')}/{source}/{d.isoformat()}"
        fs.delete(folder)
        for name, data in files.items():
            fs.write_bytes(f"{folder}/{name}", data)
        fs.write_bytes(f"{folder}/_manifest.json", manifest(source, d, files, ents))
        written[source] = len(files)
    fs.write_bytes(f"{landing.rstrip('/')}/_truth/persons.json", json.dumps(world.truth(), indent=1).encode())
    return written


def main(argv=None, spark=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--business-date", type=C.parse_date)
    p.add_argument("--all-dates", action="store_true")
    p.add_argument("--landing", default=C.DEFAULT_LANDING)
    p.add_argument("--customers", type=int, default=CFG["customers"])
    p.add_argument("--seed", type=int, default=CFG["seed"])
    args, _ = p.parse_known_args(argv)
    dates = DATES if args.all_dates else [args.business_date]
    if dates == [None]:
        p.error("--business-date or --all-dates")
    if dates[0] not in DATES:
        p.error(f"business dates are {CFG['business_dates']}")
    if spark is None and not args.landing.startswith(("file:", "/")) and "://" in args.landing:
        spark = C.get_spark("gdl-land-sources")
    fs = C.filesystem(spark, args.landing)
    world = World(args.customers, args.seed)
    for d in dates:
        print(f"{d}: landed {land(world, d, args.landing, fs)} under {args.landing}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
