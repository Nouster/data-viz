import pandas as pd
import psycopg2

df = pd.read_excel("data/pyramide-2020-TP.xls", engine="xlrd", skiprows=1)
# print(df.head())

df.columns = ["Annee", "Age", "Nombre_Hommes", "Nombre_Femmes"]

df = df[df["Annee"].astype(str).str.isnumeric()] 
df["Annee"] = df["Annee"].astype(int)  

df = df.dropna() 
df["Annee"] = df["Annee"].astype(int)
df["Nombre_Hommes"] = df["Nombre_Hommes"].astype(int)
df["Nombre_Femmes"] = df["Nombre_Femmes"].astype(int)

df["Total"] = df["Nombre_Hommes"] + df["Nombre_Femmes"]

print(df.head())

conn = psycopg2.connect(
    dbname="oildb",
    user="user1",
    password="mdpUser1",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS population (
        annee INT PRIMARY KEY,
        nombre_hommes INT,
        nombre_femmes INT,
        total INT
    )
""")
conn.commit()

for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO population (annee, nombre_hommes, nombre_femmes, total)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (annee) DO UPDATE 
        SET nombre_hommes = EXCLUDED.nombre_hommes, 
            nombre_femmes = EXCLUDED.nombre_femmes, 
            total = EXCLUDED.total
    """, (row["Annee"], row["Nombre_Hommes"], row["Nombre_Femmes"], row["Total"]))

conn.commit()
cur.close()
conn.close()