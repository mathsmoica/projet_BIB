import json
import mysql.connector


try:
    # Connexion à MariaDB
    conn = mysql.connector.connect(
        host="mariadb",
        user="root",
        password="Me170606.M@this",
        database="bible_db"
    )
    cursor = conn.cursor()
    print("✅ Connexion réussie !")

    # 🔧 Fixer la collation pour éviter l'erreur
    # cursor.execute("SET collation_connection = 'utf8mb4_0900_ai_ci'")
    # cursor.execute("SET NAMES 'utf8mb4'")
    # Définir l'encodage approprié pour MariaDB
    cursor.execute("SET NAMES 'utf8mb4'")
    cursor.execute("SET CHARACTER SET utf8mb4")
    cursor.execute("SET collation_connection = 'utf8mb4_unicode_ci'")

    # Charger le JSON
    with open("segond_1910.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    # Cache pour éviter les doublons
    livres_inserts = set()
    chapitres_inserts = set()

    for entry in data:
        id_livre = entry["book"]
        nom_livre = entry["book_name"]
        numero_chapitre = entry["chapter"]
        numero_verset = entry["verse"]
        texte_verset = entry["text"]

        # Insérer le livre une seule fois
        if id_livre not in livres_inserts:
            cursor.execute("""
                INSERT IGNORE INTO livres (id_livre, nom_livre)
                VALUES (%s, %s)
            """, (id_livre, nom_livre))
            livres_inserts.add(id_livre)

        # Insérer le chapitre une seule fois
        chapitre_key = (id_livre, numero_chapitre)
        if chapitre_key not in chapitres_inserts:
            cursor.execute("""
                INSERT IGNORE INTO chapitres (id_livre, numero_chapitre)
                VALUES (%s, %s)
            """, chapitre_key)
            chapitres_inserts.add(chapitre_key)

        # Insérer le verset
        cursor.execute("""
            INSERT INTO versets (id_livre, numero_chapitre, numero_verset, texte_verset)
            VALUES (%s, %s, %s, %s)
        """, (id_livre, numero_chapitre, numero_verset, texte_verset))

    conn.commit()
    print("📘 Importation terminée avec succès.")

except mysql.connector.Error as err:
    print(f"❌ Erreur de connexion ou d'exécution : {err}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
