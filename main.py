from requests import get, exceptions
from datetime import date
from dateutil.relativedelta import relativedelta
from os import listdir, system, name, remove, stat, path
from zipfile import ZipFile
from chardet import detect
import pandas as pd

from dico import DICTIONNAIRE
from dpts import DEPARTMENTS


# Paramétrage de l'affichage terminal
pd.set_option("display.max_columns", None)    # Affiche toutes les colonnes au lieu d'avoir '...'
pd.set_option("display.max_rows", None)       # Affiche toutes les lignes au lieu d'avoir '...'
pd.set_option("display.max_colwidth", None)   # Affiche l'intégralité du contenu d'une case
pd.set_option("display.width", None)          # Affiche toutes les colonnes sur une même rangée


# 1. Si nécessaire, télécharge le dossier RNA le plus récent
r = get("https://www.data.gouv.fr/fr/datasets/repertoire-national-des-associations/")
if r.status_code != 200:
    raise exceptions.RequestException(f"Datasets response status = {r.status_code}")

# TODO : rendre le scrapping plus souple
d = date.today()
while f"https://media.interieur.gouv.fr/rna/rna_waldec_{d.strftime('%Y%m')}" not in r.text:
    d -= relativedelta(months=1)

index = r.text.find(f"https://media.interieur.gouv.fr/rna/rna_waldec_{d.strftime('%Y%m')}")
url = r.text[index:index + 59]
filename = url.split('/')[-1]

if filename[:-4] not in listdir("data") and filename not in listdir("data"):
    with open(f"data/{filename}", "wb") as zipdata:
        r = get(url, allow_redirects=True, stream=True)

        if r.status_code != 200:
            raise exceptions.RequestException(f"rna_waldec_{d.strftime('%Y%m')} response status = {r.status_code}")
        if r.headers.get("content-length") is None:
            zipdata.write(r.content)

        dl = 0
        ts = int(r.headers.get("content-length"))

        for data in r.iter_content(chunk_size=32768):
            dl += len(data)
            prct = dl * 100 / ts
            system("cls" if name == "nt" else "clear")
            print(f"{prct:.1f}% |{'█' * int(prct / 5)}{' ' * int(20 - prct / 5)}| {dl / 1_000_000:.1f}/{ts / 1_000_000:.1f} Mo")

            zipdata.write(data)

if filename[:-4] not in listdir("data"):
    with ZipFile(f"data/{filename}") as unzipdata:
        unzipdata.extractall("data")
    remove(f"data/{filename}")

del filename, index, url


# 2. Récupère le dossier RNA Waldec le plus récent
rna_dir = [x for x in listdir("data") if x.startswith("rna_waldec")]

if len(rna_dir) <= 0:
    raise FileNotFoundError("Dossier RNA Waldec absent")
elif len(rna_dir) == 1:
    rna_dir = rna_dir[0]
else:
    rna_dir = max(rna_dir)


# 3. Récupère le fichier le moins lourd du dossier
file_list = listdir(f"data/{rna_dir}")
if ".DS_Store" in file_list:
    file_list.remove(".DS_Store")

smallest = min(file_list, key=lambda x: stat(f"data/{rna_dir}/{x}").st_size)


# 4. Récupère l'encodage des fichiers originaux
with open(f"data/{rna_dir}/{smallest}", 'rb') as rawdata:
    encoding = detect(rawdata.read())

del smallest, rawdata


# 5. Ouvre le fichier excel de destination
result = pd.ExcelWriter(f"output/{rna_dir}_esiea.xlsx")


# 6. Cherche les occurrences de 'ESIEA'
file_list.sort()
nb_dpts = len(file_list)

dpts_esiea = []
rows_found = 0
dpts_found = 0
nb_asso = 0
data_size = 0

# TODO : Rechercher par adresse
# TODO : Faire un prétraitement des données
# TODO : Optimiser les calculs
# TODO : Multi-threader les processus
# TODO : implémenter la distance de Levenshtein ou les n-grammes
for i, f in enumerate(file_list):
    # 6.1. Affichage du chargement
    system("cls" if name == "nt" else "clear")

    prct = i * 100 / nb_dpts
    print(f"{prct:.1f}% |{'█' * int(prct / 5)}{' ' * int(20 - prct / 5)}| {i}/{nb_dpts}")
    print(f"{rows_found} associations dans {dpts_found} départements")
    print(f"{nb_asso} déjà analysées")

    # 6.2. Ouverture du CSV encodé (normalement) en Windows-1252
    df = pd.read_csv(f"data/{rna_dir}/{f}",
                     sep=";",                           # Valeurs séparées par des points-virgules
                     encoding=encoding["encoding"],     # Utilise l'encodage Windows-1252 en temps normal
                     encoding_errors="replace",         # Remplace les erreurs � (U+FFFD) en cas d'erreur
                     on_bad_lines="warn",               # Si l'erreur persiste : warning + donnée skippée
                     low_memory=False)                  # Désactive le parsing par chunks
    df = df.astype(str)

    # 6.3. Supprime les lignes sans occurrences
    mask = df.applymap(lambda x: "ESIEA".lower() in x.lower())  # Recherche la chaine "ESIEA"
    # mask = df.applymap(lambda x: any([y.lower() in x.lower() for y in DICTIONNAIRE]))  # Recherche avec dictionnaire
    mask = df[mask.any(1)]
    mask = mask.replace('nan', '', regex=True)

    # 6.4. Enregistre les statistiques pour l'affichage
    data_size += path.getsize(f"data/{rna_dir}/{f}")
    nb_asso += len(df)
    as_esiea = len(mask) > 0
    dpt_number = f.split("_")[-1].split(".")[0]

    if as_esiea:
        dpts_esiea.append((dpt_number, DEPARTMENTS[dpt_number], len(mask)))
        rows_found += len(mask)
        dpts_found += 1

        # 6.5. Ajouter un département au fichier Excel
        mask.to_excel(result, sheet_name=f"dpt_{dpt_number}", index=False)


# 7. Sauvegarde du fichier Excel
result.save()


# 8. Affichage final
system("cls" if name == "nt" else "clear")

print(f"100.0% |{'█' * 20}| {nb_dpts}/{nb_dpts}")
print(f"{rows_found} associations dans {dpts_found} départements")
print(f"{nb_asso} déjà analysées")

for d in dpts_esiea:
    print(f"{d[1]}  ({d[0]}) : {d[2]}")

print(f"\nLe fichier Excel '{rna_dir}_esiea.xlsx' a bien été créé.")

with open(f"output/{rna_dir}_esiea_data.txt", "w") as data:
    data.write(f"{nb_dpts} départements analysés\n")
    data.write(f"{nb_asso} associations analysées\n")
    data.write(f"{(data_size / 1000000):.1f} Mo de données analysées\n\n")

    data.write(f"{rows_found} association(s) mentionnant l'ESIEA\n")
    data.write(f"{dpts_found} département(s) regroupant ces association(s)\n\n")
    data.write("Répartition des départements :\n")
    for d in dpts_esiea:
        data.write(f"{d[1]}  ({d[0]}) : {d[2]} association(s)\n")
