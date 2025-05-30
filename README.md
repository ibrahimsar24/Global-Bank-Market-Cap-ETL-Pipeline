
# Global Bank Market Capitalization ETL Pipeline

## 📌 Overview

This project is a simple ETL (Extract, Transform, Load) pipeline built using Python. It scrapes market capitalization data of the largest global banks from a historical snapshot of a Wikipedia page, transforms the data into multiple currencies using exchange rates from a CSV file, and loads the final dataset into both a CSV file and a SQLite database. The pipeline also runs a few SQL queries to demonstrate the final data usage.

---

## 🚀 ETL Flow

1. **Extract**:
   - Scrapes a table of global banks from a Wikipedia archive using `requests` and `BeautifulSoup`.
   - Extracted data includes bank name and market capitalization in USD.

2. **Transform**:
   - Reads exchange rates from a local CSV file.
   - Adds columns converting market capitalization to GBP, EUR, and INR using the exchange rates.

3. **Load**:
   - Writes the final dataset to a CSV file (`Largest_banks.csv`).
   - Loads the dataset into a SQLite database (`Banks.db`).
   - Executes sample SQL queries for validation and exploration.

---

## 🛠️ Technologies Used

- Python
- Pandas
- NumPy
- BeautifulSoup (`bs4`)
- SQLite3
- Requests

---

## 📁 Project Structure

```
Global-Bank-Market-Cap-ETL-Pipeline/
│
├── exchange_rate.csv               # CSV file containing currency exchange rates
├── code_log.txt                    # Log file generated during pipeline run
├── Largest_banks.csv              # Final output CSV file
├── Banks.db                        # SQLite database storing the final table
└── etl_pipeline.py                 # Main Python script for ETL process
```

---

## ⚙️ How to Run

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/Global-Bank-Market-Cap-ETL-Pipeline.git
   cd Global-Bank-Market-Cap-ETL-Pipeline
   ```

2. Install required Python libraries:
   ```bash
   pip install pandas numpy beautifulsoup4 requests
   ```

3. Prepare the `exchange_rate.csv` file with the following format:

   | Currency | Rate |
   |----------|------|
   | GBP      | 0.8  |
   | EUR      | 0.9  |
   | INR      | 83.2 |

4. Run the ETL script:
   ```bash
   python etl_pipeline.py
   ```

---

## 📊 Example SQL Queries

- List all banks:
  ```sql
  SELECT * FROM Largest_banks;
  ```

- Average market cap in GBP:
  ```sql
  SELECT AVG(MC_GBP_Billion) FROM Largest_banks;
  ```

- First 5 banks by name:
  ```sql
  SELECT Name FROM Largest_banks LIMIT 5;
  ```

---

## 🧾 Logging

All steps are logged with timestamps to `code_log.txt`.

---

## 📄 License

This project is licensed under the MIT License.

---

## 🙌 Acknowledgments

- [Wikipedia](https://en.wikipedia.org/wiki/List_of_largest_banks) for the source data
- Python open-source community

