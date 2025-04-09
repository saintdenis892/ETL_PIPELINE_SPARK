from datetime import datetime
from pyspark.sql.functions import (udf, col, year, month, dayofmonth, hour, 
                                   weekofyear, date_format, dayofweek, expr)
import configparser
import os
from pyspark.sql import SparkSession

# Chargement de la configuration AWS depuis le fichier de configuration
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Crée une session Spark avec les configurations nécessaires
    pour le traitement des fichiers S3.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Charge les données de chansons, crée les tables 'songs' et 'artists',
    et les écrit en fichiers Parquet dans un bucket S3.

    Paramètres :
    - spark : La session Spark
    - input_data : Chemin d'entrée des données (S3)
    - output_data : Chemin de sortie des données transformées (S3)
    """
    # Chargement des fichiers JSON contenant les données de chansons
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # Création de la table 'songs' et suppression des doublons
    songs_table = song_df.select('song_id', 'title', 'artist_id', 'year', 'duration') \
                         .dropDuplicates()
    
    # Écriture de la table 'songs' en fichiers Parquet, partitionnés par 'year' et 'artist_id'
    songs_table.write.partitionBy('year', 'artist_id') \
                     .mode('overwrite') \
                     .parquet(output_data + "songs/songs_table.parquet")

    # Création de la table 'artists' et suppression des doublons
    artists_table = song_df.selectExpr('artist_id', 'artist_name', 
                                       'artist_location', 'artist_latitude', 
                                       'artist_longitude').dropDuplicates()
    
    # Écriture de la table 'artists' en fichiers Parquet
    artists_table.write.mode('overwrite') \
                       .parquet(output_data + "artists/artists_table.parquet")

def process_log_data(spark, input_data, output_data):
    """
    Charge les données de logs, crée les tables 'users', 'time', 'songplays',
    et les écrit en fichiers Parquet dans un bucket S3.

    Paramètres :
    - spark : La session Spark
    - input_data : Chemin d'entrée des données (S3)
    - output_data : Chemin de sortie des données transformées (S3)
    """
    # Chargement des fichiers JSON contenant les logs
    log_data = input_data + "log_data/2018/11/*.json"
    log_df = spark.read.json(log_data)

    # Filtrage des actions 'NextSong' (lecture de chanson)
    log_df = log_df.filter(log_df.page == 'NextSong')

    # Création de la table 'users' avec suppression des doublons
    users_table = log_df.selectExpr(
        'userId as user_id', 'firstName as first_name', 
        'lastName as last_name', 'gender', 'level'
    ).dropDuplicates(['user_id'])

    # Écriture de la table 'users' en fichiers Parquet
    users_table.write.mode('overwrite') \
                     .parquet(output_data + "users/users_table.parquet")

    # Ajout d'une colonne 'timestamp' à partir de 'ts'
    log_df = log_df.withColumn('timestamp', expr("cast(ts/1000 as timestamp)"))

    # Création de la table 'time' à partir des colonnes de date/heure
    time_table = log_df.select(
        col('timestamp').alias('start_time'),
        hour('timestamp').alias('hour'),
        dayofmonth('timestamp').alias('day'),
        weekofyear('timestamp').alias('week'),
        month('timestamp').alias('month'),
        year('timestamp').alias('year'),
        dayofweek('timestamp').alias('weekday')
    ).dropDuplicates()

    # Écriture de la table 'time' en fichiers Parquet, partitionnés par 'year' et 'month'
    time_table.write.partitionBy('year', 'month') \
                    .mode('overwrite') \
                    .parquet(output_data + "time/time_table.parquet")

    # Chargement des données de chansons pour créer la table 'songplays'
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # Création de la table 'songplays' avec jointure entre 'song_data' et 'log_data'
    songplays_table = log_df.join(
        song_df, 
        (log_df.artist == song_df.artist_name) & 
        (log_df.song == song_df.title) & 
        (log_df.length == song_df.duration), 
        'inner'
    ).select(
        expr("row_number() over(order by log_df.ts)").alias('songplay_id'),
        col('timestamp').alias('start_time'),
        col('userId').alias('user_id'),
        'level', 'song_id', 'artist_id', 
        col('sessionId').alias('session_id'), 
        'location', col('userAgent').alias('user_agent'),
        year('timestamp').alias('year'),
        month('timestamp').alias('month')
    )

    # Écriture de la table 'songplays' en fichiers Parquet, partitionnés par 'year' et 'month'
    songplays_table.write.partitionBy('year', 'month') \
                         .mode('overwrite') \
                         .parquet(output_data + "songplays/songplays_table.parquet")

def main():
    """
    Point d'entrée principal du programme.
    Crée la session Spark, puis traite les données de chansons et de logs.
    """
    spark = create_spark_session()

    # Chargement des chemins d'entrée et de sortie depuis le fichier de configuration
    input_data = config.get('IO', 'INPUT_DATA')
    output_data = config.get('IO', 'OUTPUT_DATA')

    # Traitement des données de chansons et de logs
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

# Exécution du script si c'est le fichier principal
if __name__ == "__main__":
    main()
