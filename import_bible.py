import json
import mysql.connector

def main():
    try:
        # Connexion à MariaDB
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Me170606.M@this",
            database="bible_db"
        )
        
        cursor = conn.cursor()
        print("✅ Connexion réussie à MariaDB!")

        # Définir l'encodage approprié pour MariaDB
        cursor.execute("SET NAMES 'utf8mb4'")
        cursor.execute("SET CHARACTER SET utf8mb4")
        cursor.execute("SET collation_connection = 'utf8mb4_unicode_ci'")

        # Charger le JSON
        with open("segond_1910.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        # Créer les tables si elles n'existent pas
        create_tables(cursor)

        # Cache pour éviter les doublons
        livres_inserts = set()
        chapitres_inserts = set()

        print("📘 Début de l'importation des données...")

        for i, entry in enumerate(data):
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

            # Afficher la progression
            if (i + 1) % 1000 == 0:
                print(f"✅ {i + 1} versets importés...")

        conn.commit()
        print("📘 Importation terminée avec succès!")
        print(f"📊 {len(livres_inserts)} livres, {len(chapitres_inserts)} chapitres, {len(data)} versets importés.")

    except mysql.connector.Error as err:
        print(f"❌ Erreur de connexion ou d'exécution : {err}")

    except FileNotFoundError:
        print("❌ Fichier segond_1910.json non trouvé.")
        print("💡 Assurez-vous que le fichier est dans le même répertoire que ce script.")

    except Exception as e:
        print(f"❌ Erreur inattendue : {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def create_tables(cursor):
    """Crée les tables si elles n'existent pas"""
    
    # Table des livres
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS livres (
            id_livre INT PRIMARY KEY,
            nom_livre VARCHAR(50) NOT NULL
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    """)
    
    # Table des chapitres
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chapitres (
            id_chapitre INT AUTO_INCREMENT PRIMARY KEY,
            id_livre INT NOT NULL,
            numero_chapitre INT NOT NULL,
            FOREIGN KEY (id_livre) REFERENCES livres(id_livre),
            UNIQUE KEY unique_chapitre (id_livre, numero_chapitre)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    """)
    
    # Table des versets
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS versets (
            id_verset INT AUTO_INCREMENT PRIMARY KEY,
            id_livre INT NOT NULL,
            numero_chapitre INT NOT NULL,
            numero_verset INT NOT NULL,
            texte_verset TEXT NOT NULL,
            FOREIGN KEY (id_livre) REFERENCES livres(id_livre),
            INDEX chapitre_verset_idx (id_livre, numero_chapitre, numero_verset)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    """)
    
    print("✅ Tables créées ou déjà existantes")

if __name__ == "__main__":
    main()