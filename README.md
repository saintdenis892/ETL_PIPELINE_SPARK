# Sparkify ETL Project  

## 1. Objectif  
Ce projet vise à créer un **pipeline ETL** pour Sparkify, une plateforme de streaming musical. Le pipeline extrait les données de **logs utilisateur et de chansons** depuis S3, les traite avec **Spark**, et les recharge dans S3 sous forme de tables dimensionnelles. Cela permet à l’équipe d’analyse d’obtenir des insights sur les habitudes d’écoute des utilisateurs.  

---

## 2. Jeux de données  
Les données sources se trouvent dans S3 :  
- **Données des chansons** :  
  `s3://udacity-dend/song_data`  
  Contient des informations sur les chansons et artistes (extrait du Million Song Dataset).  
- **Données des logs** :  
  `s3://udacity-dend/log_data`  
  Simule l’activité utilisateur sur l’application Sparkify.  

---

## 3. Schéma de la base de données  
Le schéma suit le modèle **en étoile** avec une table de faits et plusieurs dimensions.  

### 3.1 Table de faits  
- **songplays** : Enregistre les événements où une chanson est jouée.  
  **Colonnes** : songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent  

### 3.2 Tables de dimensions  
- **users** : Infos utilisateur (user_id, first_name, last_name, gender, level)  
- **songs** : Infos chanson (song_id, title, artist_id, year, duration)  
- **artists** : Infos artiste (artist_id, name, location, latitude, longitude)  
- **time** : Découpe des timestamps (start_time, hour, day, week, month, year, weekday)  

---

## 4. Pipeline ETL  
1. **Extraction** : Chargement des données de S3 dans des DataFrames Spark.  
2. **Transformation** : Filtrage, nettoyage et création des tables `songplays`, `songs`, `artists`, `users`, et `time`.  
3. **Chargement** : Écriture des tables transformées dans S3 (avec partitionnement pour certaines).  

---

## 5. Exemple de log utilisateur  
| artist                     | firstName | gender | page     | song                           | level | location                        | ts            |  
|----------------------------|-----------|--------|----------|--------------------------------|-------|---------------------------------|---------------|  
| Pavement                   | Sylvie    | F      | NextSong | Mercy: The Laundromat         | free  | Washington-Arlington-Alexandria | 1541990258796 |  
| Gary Allan                 | Celeste   | F      | NextSong | Nothing On But The Radio      | free  | Klamath Falls, OR              | 1541990541796 |  

---

## 6. Exécution du Projet  
1. Configurer les **buckets S3** et s’assurer que Spark est bien installé.  
2. Lancer le pipeline ETL avec Spark :  
   ```bash
   spark-submit etl.py
