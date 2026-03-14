---

## Day 1

# Day 1 — Foundations: OOP, File Handling & Multi-Format Parsing

**Goal:** Build the ingestion layer. By end of Day 1, you should be able to point your pipeline at the `inputs/` folder and get raw Python dictionaries out of any of the 5 file formats.

**Concepts practiced:** OOP (abstract classes, inheritance, dataclasses), File Handling, CSV/JSON/XML/YAML/regex parsing

**Estimated time:** 4–5 hours

---

### Step 1.1 — Create the pipeline configuration (`pipeline/config.py`)

Every production pipeline has a config object. This avoids hardcoding paths, batch sizes, and run metadata all over the codebase.

**Why use a dataclass?** A `@dataclass` gives you `__init__`, `__repr__`, and type annotations for free — it is the cleanest way to represent a bag of settings in Python 3.7+.

```python
# pipeline/config.py
from dataclasses import dataclass, field
from datetime import datetime
import os

@dataclass
class PipelineConfig:
    input_dir: str
    output_dir: str
    batch_size: int = 50
    run_id: str = field(default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S"))

    def __post_init__(self):
        # Automatically create the output directory if it doesn't exist
        # exist_ok=True means no error if the folder is already there
        os.makedirs(self.output_dir, exist_ok=True)

    @property
    def consolidated_csv_path(self):
        return os.path.join(self.output_dir, "consolidated.csv")

    @property
    def summary_json_path(self):
        return os.path.join(self.output_dir, "summary.json")

    @property
    def rejected_csv_path(self):
        return os.path.join(self.output_dir, "rejected_records.csv")

    @property
    def audit_log_path(self):
        return os.path.join(self.output_dir, "audit.log")
```

**What to observe:**
- `@dataclass` auto-generates `__init__` — you do not write it manually
- `field(default_factory=...)` is used for mutable defaults (never use `default=[]` in dataclasses)
- `__post_init__` is called after `__init__` — perfect for side effects like directory creation
- `@property` exposes computed paths without storing them — if `output_dir` changes, the paths update automatically

---

### Step 1.2 — Build the abstract base class (`pipeline/ingesters.py` — Part 1)

This is the most important OOP decision in the project. You define a contract: every ingester must implement `ingest()`. The pipeline does not care whether it is talking to a CSV file or an XML file — it only calls `ingest()`.

**Why an abstract class?** It enforces the interface. If someone writes a new `ExcelIngester` and forgets to implement `ingest()`, Python will raise a `TypeError` at class definition time — not at runtime when the pipeline is already running.

```python
# pipeline/ingesters.py
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator
import csv, json, xml.etree.ElementTree as ET, yaml, re, os

@dataclass
class BaseIngester(ABC):
    """
    Abstract base class for all file ingesters.
    Every subclass MUST implement the ingest() method.
    """
    file_path: str
    source_name: str = ""

    def __post_init__(self):
        if not self.source_name:
            self.source_name = os.path.basename(self.file_path)

    @abstractmethod
    def ingest(self) -> Iterator[dict]:
        """
        Open the file, parse it, and yield one record as a dict at a time.
        Must be a generator — do NOT return a list.
        """
        pass

    def _base_metadata(self) -> dict:
        """Adds tracking fields to every record, regardless of source format."""
        return {
            "source_file": self.source_name,
            "ingested_at": datetime.now().isoformat()
        }
```

**What to observe:**
- `ABC` and `@abstractmethod` together mean you cannot instantiate `BaseIngester()` directly
- The `_base_metadata()` helper is prefixed with `_` — Python convention for "internal use only"
- Returning `Iterator[dict]` as the type hint tells every future developer: this must be a generator

---

### Step 1.3 — Build the 5 concrete ingesters (`pipeline/ingesters.py` — Part 2)

Now implement a subclass for each file format. Each one inherits `file_path`, `source_name`, and `_base_metadata()` from the base class and only needs to implement `ingest()`.

#### CSVIngester

```python
class CSVIngester(BaseIngester):
    """
    Reads comma-separated files using csv.DictReader.
    DictReader automatically uses the first row as column headers.
    """
    def ingest(self) -> Iterator[dict]:
        with open(self.file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                record = dict(row)           # row is an OrderedDict — convert to plain dict
                record.update(self._base_metadata())
                yield record                 # yield one at a time — do not collect into a list
```

**Why `with open(...)` instead of `f = open(...)`?**
The `with` statement is a context manager. It guarantees the file is closed even if an exception is raised inside the block. Without it, a crash mid-read leaves the file handle open — on large systems, this causes "too many open files" errors.

#### JSONIngester

```python
class JSONIngester(BaseIngester):
    """
    Handles two JSON shapes:
    - A JSON array: [ {...}, {...}, {...} ]
    - Newline-delimited JSON (NDJSON): one JSON object per line
    """
    def ingest(self) -> Iterator[dict]:
        with open(self.file_path, mode='r', encoding='utf-8') as f:
            content = f.read().strip()

        # Detect format: if it starts with '[', it's a JSON array
        if content.startswith('['):
            records = json.loads(content)
            for record in records:
                record.update(self._base_metadata())
                yield record
        else:
            # Newline-delimited JSON — parse line by line
            for line in content.splitlines():
                if line.strip():
                    record = json.loads(line)
                    record.update(self._base_metadata())
                    yield record
```

#### XMLIngester

```python
class XMLIngester(BaseIngester):
    """
    Parses XML using the standard library's ElementTree.
    Assumes each child of the root element is one shipment record.

    Example structure:
    <shipments>
        <shipment>
            <shipment_id>SH001</shipment_id>
            <status>DELIVERED</status>
        </shipment>
    </shipments>
    """
    def ingest(self) -> Iterator[dict]:
        tree = ET.parse(self.file_path)
        root = tree.getroot()

        for child in root:
            # Convert all XML child tags into a flat dictionary
            record = {element.tag: element.text for element in child}
            record.update(self._base_metadata())
            yield record
```

#### YAMLIngester

```python
class YAMLIngester(BaseIngester):
    """
    Reads YAML files using PyYAML.
    yaml.safe_load_all() handles multi-document YAML streams (separated by ---).
    Always use safe_load, never yaml.load() — it can execute arbitrary code.
    """
    def ingest(self) -> Iterator[dict]:
        with open(self.file_path, mode='r', encoding='utf-8') as f:
            documents = yaml.safe_load_all(f)   # returns a generator of documents
            for doc in documents:
                if isinstance(doc, list):
                    for record in doc:
                        record.update(self._base_metadata())
                        yield record
                elif isinstance(doc, dict):
                    doc.update(self._base_metadata())
                    yield doc
```

#### LogIngester

```python
class LogIngester(BaseIngester):
    """
    Parses plain-text log files using regular expressions.
    Each line looks like:
    [2024-01-15] ORDER_ID=SH1234 STATUS=DELIVERED WEIGHT=12.4kg ORIGIN=Mumbai DEST=Delhi

    This is common in older logistics systems that export flat log files.
    """
    # Compile the regex once at class level — not inside ingest() — for performance
    LINE_PATTERN = re.compile(
        r'\[(?P<date>[\d-]+)\]\s+'
        r'ORDER_ID=(?P<shipment_id>\S+)\s+'
        r'STATUS=(?P<status>\S+)\s+'
        r'WEIGHT=(?P<weight>\S+)\s+'
        r'ORIGIN=(?P<origin_city>\S+)\s+'
        r'DEST=(?P<destination_city>\S+)'
    )

    def ingest(self) -> Iterator[dict]:
        with open(self.file_path, mode='r', encoding='utf-8') as f:
            for line_number, line in enumerate(f, start=1):
                line = line.strip()
                if not line or line.startswith('#'):  # skip blank lines and comments
                    continue
                match = self.LINE_PATTERN.match(line)
                if match:
                    record = match.groupdict()
                    record.update(self._base_metadata())
                    yield record
                # Lines that don't match are silently skipped here
                # Day 3 will add proper error handling for this
```

---

### Step 1.4 — Build an ingester factory function

Add this at the bottom of `ingesters.py`. It maps file extensions to the correct class so the pipeline never needs to care about formats.

```python
def get_ingester(file_path: str) -> BaseIngester:
    """
    Factory function: given a file path, returns the right ingester.
    Raises ValueError for unsupported formats.
    """
    ext = os.path.splitext(file_path)[1].lower()
    mapping = {
        '.csv':  CSVIngester,
        '.json': JSONIngester,
        '.xml':  XMLIngester,
        '.yaml': YAMLIngester,
        '.yml':  YAMLIngester,
        '.txt':  LogIngester,
        '.log':  LogIngester,
    }
    if ext not in mapping:
        raise ValueError(f"Unsupported file format: '{ext}' for file: {file_path}")
    return mapping[ext](file_path=file_path)
```

---

### Step 1.5 — Verify Day 1 works

Create a quick test script (not part of the final project — just for verification):

```python
# test_day1.py  (delete after verification)
from pipeline.ingesters import get_ingester

test_file = "data/inputs/north_region.csv"
ingester = get_ingester(test_file)

for i, record in enumerate(ingester.ingest()):
    print(record)
    if i >= 2:  # print first 3 records only
        break
```

You should see 3 dictionaries printed with a `source_file` and `ingested_at` key added automatically.

---

### Day 1 Summary — What You Learned and Why It Matters

**OOP gave you plug-and-play extensibility.** When your manager says "we have a new Parquet source from the data warehouse," you create one new class `ParquetIngester(BaseIngester)`, implement `ingest()`, add `.parquet` to the factory, and the rest of the pipeline keeps running unchanged. This is the Open/Closed Principle — open for extension, closed for modification.

**File handling with context managers is non-negotiable.** Every professional Python codebase uses `with open(...)`. The `with` statement ensures cleanup even during failures. In a pipeline that runs every night, a leaked file handle is a slow-burning bug that only shows up weeks later at 3 AM.

**Multi-format parsing is a real data engineering skill.** You just wrote code that handles 5 completely different serialisation formats using only Python's standard library (except PyYAML). More importantly, you encapsulated each format's logic in its own class — so a bug in XML parsing cannot affect CSV parsing.

**The key mental model from Day 1:** Think in terms of contracts, not implementations. The pipeline does not know or care about CSV, XML, or YAML. It only calls `ingest()` and gets records. This is the foundation of maintainable software.

---