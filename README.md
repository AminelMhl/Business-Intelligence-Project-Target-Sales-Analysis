# Business Intelligence Project

=====================================

## **Overview**


This project is designed to extract, transform, and load (ETL) data from various sources into a data warehouse for business intelligence purposes. The project utilizes Python as the primary programming language and leverages various libraries such as Pandas, NumPy, and SQLAlchemy for data manipulation and database interactions.

---

## **Project Structure**


- **`scripts`**: Contains Python scripts for data extraction, transformation, and loading.
- **`data`**: Stores raw and processed data files.
- **`Modeling and Storage`**: Stores data models and storage files.

---

## **Requirements**


- Python 3.x
- Pandas
- NumPy
- SQLAlchemy
- OpenPyXL
- Scikit-learn
- Matplotlib
- Seaborn

---

## **Installation**


To install the required dependencies, run the following command:

```
bashInsert CodeRunCopy code
1pip install -r requirements.txt

```

## **Usage**


To run the ETL pipeline, execute the following command:

```
bashInsert CodeRunCopy code
1python scripts/run_etl.py

```

This will extract data from local files, transform the data, and load it into a data warehouse.

## **Data Sources**


- **`order_items.csv`**
- **`payments.csv`**
- **`orders.csv`**
- **`sellers.json`**
- **`products.csv`**
- **`customers.json`**

---

## **Data Warehouse**


The project uses a SQLite database as the data warehouse. The database is created and populated with data during the ETL process.

---

## **Data Models**


- **`customer_dimension`**
- **`payment_dimension`**
- **`seller_dimension`**
- **`product_dimension`**
- **`fact_order`**

---

## **License**


This project is licensed under the MIT License.

---

## **Acknowledgments**


This project was created by Mohamed Amine Soltana, Rayen Laabidi and Achref Msekni
