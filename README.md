
Installation


    docker run -it -v $PWD:/userdata -p 8088:8088 -p 8042:8042 -h sandbox sequenceiq/spark:1.6.0 bash
    cd /userdata
    spark-submit simplespark.py
