# Pension System Manager

This is a hybrid data management application that integrates a relational SQL Server database with a Redis NoSQL store to manage pensioners, banks, transactions, and metadata.

## 🔧 Technologies Used

- Python 3.x
- SQL Server
- Redis (with JSON support)
- PrettyTable
- Matplotlib / Pandas
- PyODBC
- Cryptography (Fernet encryption)

## 🗃️ Features

- Full SQL CRUD operations on Pensioner and related entities
- Automatic Redis mirror (with encrypted sensitive fields)
- Metadata generation (device, language, login count)
- Cascading deletes with conflict handling
- Interactive terminal menu
- Visualization of Redis metadata

## 🧪 Setup & Running

1. Clone the repository:
   ```bash
   git clone https://github.com/arfa-10/pension-system.git
   cd pension-system


pip install -r requirements.txt

export REDIS_SECRET_KEY="your-key-here"

python pension_system_manager.py


*Make sure Redis is running on port 6380

*Redis must support ReJSON module for .json() to work

*Your SQL credentials should match those in the script


This repo includes:

requirements.txt

pension_system_manager.py

redis_data_synced.json (initial dataset)

Test script (test_flow.py)

README.md