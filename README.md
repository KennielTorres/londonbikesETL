
# London Bikes ETL

A  small project that covers the general process of extracting, transforming, and loading data into a database; in this case, a database inside a local Docker container.

The data used was acquired from [Kaggle](https://www.kaggle.com/datasets/edenau/london-bike-sharing-system-data) and originally sourced from [Transport of London - Cycling Data](https://cycling.data.tfl.gov.uk/). Such data was not included in the repository.


## Tech Stack
- Python
- Pandas
- Docker
- PostgreSQL

### Tools Used
- Visual Studio Code
- DataGrip
## Run Locally

Clone the project

```bash
  git clone https://github.com/KennielTorres/londonbikesETL.git
```

Install dependencies

```bash
  pip install -r requirements.txt
```

Start up your PostgreSQL database and replace the values of the config.py file with yours. Then proceed to download the data from [Kaggle](https://www.kaggle.com/datasets/edenau/london-bike-sharing-system-data), create a folder named **data** inside the project directory, and place the **journeys.csv** and **stations.csv** files inside it.

Your new folder should look like this.

- **/londonbikesETL/data/journeys.csv**
- **/londonbikesETL/data/stations.csv**

Run the script.
```bash
  python3 load_data.py
```

Using DataGrip, you will be able to connect to the database and see the results. A free and open source alternative will be to connect through pgAdmin.


## Author

- [@KennielTorres](https://www.github.com/KennielTorres)
