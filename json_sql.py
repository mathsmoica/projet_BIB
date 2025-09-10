import json

try:
    # Charger le JSON
    with open("segond_1910.json", "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    # Accéder à la liste des versets
    if "verses" not in raw_data or not isinstance(raw_data["verses"], list):
        raise ValueError("Le fichier JSON ne contient pas de liste 'verses' valide.")

    data = raw_data["verses"]

    # Ouvrir le fichier SQL en écriture
    with open("bible.sql", "w", encoding="utf-8") as sql_file:
        livres_inserts = set()
        chapitres_inserts = set()

        # Requêtes de création de tables avec contrainte UNIQUE sur les versets
        sql_file.write("""
-- Création des tables
CREATE TABLE IF NOT EXISTS livres (
  id_livre INT PRIMARY KEY,
  nom_livre VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS chapitres (
  id_chapitre INT PRIMARY KEY AUTO_INCREMENT,
  id_livre INT NOT NULL,
  numero_chapitre INT NOT NULL,
  FOREIGN KEY (id_livre) REFERENCES livres(id_livre)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS versets (
  id_verset INT PRIMARY KEY AUTO_INCREMENT,
  id_livre INT NOT NULL,
  numero_chapitre INT NOT NULL,
  numero_verset INT NOT NULL,
  texte_verset TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  FOREIGN KEY (id_livre) REFERENCES livres(id_livre)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  UNIQUE (id_livre, numero_chapitre, numero_verset)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Insertion des données
""")

        # Générer les requêtes d'insertion
        for entry in data:
            id_livre = entry["book"]
            nom_livre = entry["book_name"].replace("'", "''")
            numero_chapitre = entry["chapter"]
            numero_verset = entry["verse"]
            texte_verset = entry["text"].replace("'", "''")

            # Livre
            if id_livre not in livres_inserts:
                sql_file.write(
                    f"INSERT IGNORE INTO livres (id_livre, nom_livre) VALUES ({id_livre}, '{nom_livre}');\n"
                )
                livres_inserts.add(id_livre)

            # Chapitre
            chapitre_key = (id_livre, numero_chapitre)
            if chapitre_key not in chapitres_inserts:
                sql_file.write(
                    f"INSERT IGNORE INTO chapitres (id_livre, numero_chapitre) VALUES ({id_livre}, {numero_chapitre});\n"
                )
                chapitres_inserts.add(chapitre_key)

            # Verset (avec INSERT IGNORE pour éviter les doublons)
            sql_file.write(
                f"INSERT IGNORE INTO versets (id_livre, numero_chapitre, numero_verset, texte_verset) VALUES ({id_livre}, {numero_chapitre}, {numero_verset}, '{texte_verset}');\n"
            )

    print("✅ Fichier bible.sql généré avec succès.")

except Exception as e:
    print(f"❌ Erreur : {e}")
