# BoQ Bid Studio

Interaktivní aplikace pro mapování, porovnávání a kontrolu nabídek.

## Práce s Excel Group/Outline

Aplikace podporuje načtení a zobrazení skupin řádků/sloupců (Excel outline) na záložce **🧾 Kontrola dat**.

### Import

* Outline metadata se načítají pouze ze souborů **.xlsx** a **.xlsm**. Soubor musí být dostupný jako fyzická cesta (při nahrání ve Streamlitu se dočasně uloží do temp adresáře).
* Při načtení se vytvoří strom uzlů pro každý list a k řádkům se doplní sloupce `row_ref`, `row_outline_level` a `row_collapsed`, které uchovávají vazbu na původní Excel řádek.
* Pokud sešit outline nepoužívá, aplikace pokračuje v dosavadním chování (no-op).

### UI — panel Outline

1. V záložce **🧾 Kontrola dat** zvol list a přepni expander **Outline**.
2. Vyber sešit (Master nebo dodavatel) a osu (řádky/sloupce).
3. Strom outline zobrazuje uzly (úroveň, rozsah, sbalený stav). Kliknutím na uzel spočítáš roll-up metriky (součty množství a cen) pro danou část tabulky.
4. Pod stromem je tabulka se souhrny pro všechny uzly vybrané osy.
5. Tlačítko **Export s outline** vygeneruje XLSX soubor s identickou skupinovou strukturou.

### Export

* Export využívá `openpyxl` a při volbě „Export s outline“ zachová outline úrovně i sbalené stavy.
* Vlastnosti listu jsou nastaveny s `summaryBelow=True` a `summaryRight=True`, aby součty zůstaly pod resp. vpravo od skupin.

### Omezení

* Outline u souborů **.xls** (BIFF) není podporován.
* Export s outline vytváří nový sešit bez maker (XLSX). Pro soubory XLSM se makra nepřenášejí.
* Panel Outline je aktuálně dostupný pouze na záložce **🧾 Kontrola dat**.

## Nasazení na Railway

Postup pro nasazení aplikace, persistentní Railway Volume a propojení domény
`www.boq-studio.cz` spravované u FORPSI je popsán v
[`docs/railway_deployment.md`](docs/railway_deployment.md).
