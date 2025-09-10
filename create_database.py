import mysql.connector

def create_database():
    try:
        # Connexion à MariaDB (sans spécifier de base de données)
        conn = mysql.connector.connect(
            host="mariadb",
            user="root",
            password="Me170606.M@this",
            port=3337 
        )
        
        cursor = conn.cursor()
        print("✅ Connexion réussie à MariaDB!")

        # Créer la base de données si elle n'existe pas
        cursor.execute("CREATE DATABASE IF NOT EXISTS bible_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        print("✅ Base de données 'bible_db' créée ou déjà existante")

        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"❌ Erreur de connexion : {err}")
        print("💡 Vérifiez que MariaDB est démarré et que les paramètres de connexion sont corrects")

if __name__ == "__main__":
    create_database()