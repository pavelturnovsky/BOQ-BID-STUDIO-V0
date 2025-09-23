# BOQ Bid Studio MVP

Rozšířená aplikace pro porovnávání nabídek dodavatelů nad rozsáhlými výkazy výměr. Projekt poskytuje
CLI i Streamlit UI, modulární zpracovatelský backend, konfiguraci jednotek/měn a syntetická
ukázková data.

## Přehled funkcí

- Načítání 1–N nabídek z Excelu/CSV včetně vícelistových sešitů, automatické mapování hlaviček.
- Normalizace jednotek, převod měn na základní měnu, výpočet DPH a validace součtů.
- Detekce disciplín podle popisů položek, názvů listů a konfigurovaných synonym.
- Párování položek na uzly WBS s vysvětlením skóre (rapidfuzz + TF-IDF) a auditním logem `logs/matching.jsonl`.
- Agregace po disciplínách/WBS, označení odlehlých hodnot a hybridní vyhledávání (klíčová slova + TF-IDF).
- Export XLSX reportu (listy `Summary`, `Items`, `Unmatched`) a interaktivní Streamlit UI s heatmapou odchylek.

## Instalace

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Ukázková data

Ve složce `sample_data/` jsou připraveny dvě syntetické nabídky (`offer_A.csv`, `offer_B.csv`) a
referenční WBS strom (`master_wbs.csv`). Hodnoty obsahují kombinaci českých/anglických popisů,
různých měn i jednotek.

## CLI

```bash
python app.py --inputs sample_data/offer_A.csv sample_data/offer_B.csv --baseline median --rollup VZT --export out/report.xlsx
python app.py --inputs sample_data/offer_A.csv sample_data/offer_B.csv --query "najdi vzduchotechnickou jednotku"
```

CLI provede celý pipeline a vytiskne souhrnné tabulky. Parametr `--rollup` akceptuje kód disciplíny
nebo výraz `WBS:<prefix>` pro filtrování podle WBS. Export vytvoří XLSX report ve složce `out/`.

## Streamlit UI

```bash
streamlit run ui_app.py
```

UI umožňuje nahrát vlastní soubory nebo použít připravená ukázková data. Obsahuje filtry podle
baseline, disciplíny, WBS a outlierů, interaktivní vyhledávání i tlačítko pro stažení reportu.

## Testy

Spusťte unit testy pomocí `pytest`:

```bash
pytest
```

Testy pokrývají ingest, normalizaci, párování, agregace i vyhledávání nad ukázkovými daty.

## Struktura projektu

```
.
├── app.py                 # CLI vstup
├── ui_app.py              # Streamlit UI
├── core/                  # Moduly pipeline (ingest, normalize, matching, ...)
├── config/config.yaml     # Konfigurace mapování, disciplín, WBS
├── sample_data/           # Syntetické nabídky
├── logs/                  # Auditní logy (vytvářeno za běhu)
├── out/                   # Exporty (vytvářeno za běhu)
└── tests/                 # Pytest scénáře
```

## Auditní log

Při párování položek vzniká záznam v `logs/matching.jsonl` s informacemi o skóre, signálech a
časovém razítku. Log lze použít pro dohledání důvodu přiřazení.

## Licence

Projekt slouží jako demonstrační MVP a neobsahuje licenční omezení.
