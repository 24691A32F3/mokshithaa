# Hospital Patient Record Aggregation — Big Data Analytics

## Project overview
This student project demonstrates aggregation of hospital patient records using Apache Spark (PySpark). It calculates:
- Patient counts, average stay, and total treatment cost per department
- Patient counts per diagnosis
- Patient counts by age group and gender
- Monthly admission counts

**Privacy:** `patient_records.csv` contains 500 entirely synthetic demo records. It has no real patient data. Never upload real patient-identifiable/medical data to a public GitHub repository.

## Project files
- `patient_records.csv` — synthetic input dataset
- `run_analysis.py` — PySpark aggregation code
- `requirements.txt` — Python dependencies
- `output/` — generated CSV output folders (created after running)
- `.gitignore` — ignores generated output and local environment files

## Requirements
- Python 3.10 or 3.11 recommended
- Java JDK 17 recommended (Spark needs Java)
- Apache Spark is installed through the PySpark Python package

## Run on Windows (PowerShell)
1. Install Python and Java JDK 17. Restart the terminal after installation.
2. Check installations:
   ```powershell
   python --version
   java -version
   ```
3. In PowerShell, move into this project folder:
   ```powershell
   cd path\to\hospital_patient_record_aggregation
   ```
4. Create and activate a virtual environment:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```
   If PowerShell blocks activation, you can run the command in Command Prompt instead:
   ` .venv\Scripts\activate.bat `
5. Install dependency:
   ```powershell
   python -m pip install -r requirements.txt
   ```
6. Run:
   ```powershell
   python run_analysis.py
   ```
7. Spark displays summary tables in the terminal and writes output CSV folders inside `output/`. Each output folder contains a Spark-generated `part-*.csv` file and a `_SUCCESS` marker. Open the part CSV in Excel.

## Push project to GitHub
1. Sign in at GitHub and create a **new repository** named `hospital-patient-record-aggregation`. Choose Public or Private. Do not initialize it with a README if you plan to push this folder as-is.
2. Install Git for Windows if `git --version` is not recognized.
3. Open PowerShell in this project folder and run:
   ```powershell
   git init
   git add .
   git commit -m "Add hospital patient record aggregation project"
   git branch -M main
   git remote add origin https://github.com/YOUR-USERNAME/hospital-patient-record-aggregation.git
   git push -u origin main
   ```
4. Replace `YOUR-USERNAME` with your GitHub username. If Git asks to authenticate, follow the browser sign-in prompt.

## Troubleshooting
- `JAVA_GATEWAY_EXITED`: confirm `java -version` works and Java JDK 17 is installed; restart PowerShell.
- `ModuleNotFoundError: pyspark`: activate `.venv` and run `python -m pip install -r requirements.txt`.
- `git is not recognized`: install Git for Windows, then restart terminal.
- Do not commit `.venv/` or generated `output/` folders; `.gitignore` excludes them.

## Learning note
This is a demonstration project for coursework. Aggregation results describe the synthetic dataset only and are not medical conclusions.
