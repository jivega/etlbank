sudo docker build -t  etlbankdockimage  .
sudo docker run -i -t etlbankdockimage /bin/bash
spark-submit \
      --deploy-mode client \
      --master local \
      --class com.jivega.etlbank.driver.Ingestion \
      etlbank-1.0-SNAPSHOT.jar

spark-submit \
      --deploy-mode client \
      --master local \
      --class com.jivega.etlbank.driver.Analytics \
      etlbank-1.0-SNAPSHOT.jar
