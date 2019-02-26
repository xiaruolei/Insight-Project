spark-submit ALS.py 
spark-submit recommender.py
spark-submit mapping.py
spark-submit history.py
spark-submit --jars mysql-connector-java-8.0.15.jar load_result.py
spark-submit --jars mysql-connector-java-8.0.15.jar load_mapping.py
spark-submit --jars mysql-connector-java-8.0.15.jar load_history.py
