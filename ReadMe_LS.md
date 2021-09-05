### To build the image, run below command:

 - docker build -t web_scraper:1.0.0 -f Dockerfile .

### To run the container, run below command:

- non-dev:
    - docker run --name web_scraper_app -it --rm -v resources:/DataEngineeringProject/resources web_scraper:1.0.0

- dev only (bind mount):
    - docker run --name web_scraper_app -it --rm -v "C:\YourPath\DataEngineeringProject:
/DataEngineeringProject"  web_scraper:1.0.0

### To run the scrape service from Python, run:

 - ScrapeService_Runner.py

### Design points

#### data storage:
    - in dev mode, data is read and stored locally in .csv file.
    - in prod mode, data is read and stored locally in .parquet file which is faster and smaller.
    - maybe consider database (e.g., snowflake) for data storage.

