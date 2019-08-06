# Job_posting_data_processing_with_kafka_spark_cassandra
## Summerize
This project was aimed to implemente an end-to-end job posting data processing platform with respect to the data-related job in different US cities on Indeed 
## step by step
 - first, I scraped 8547 entries restricted to full-time and entry-level using Selenium and BeautifulSouo by running gripindeed.py, this took almost a whole day as I added certain pause point to avoid overwhelming.
 - after every job posting crawled, it was sent through kafka producer to the specific topic immediately, here we have three topics, DS,DA and DE, and for each topics, we have 8 cities as subcategories
 - at the consumer,I constructed ETL pipeline to consume data from each topics and write them into Apache Cassandra by running consumer.py
 - finally, I have analysis the data by pulling them from Cassandra and developed a dashboard using Tableau.
