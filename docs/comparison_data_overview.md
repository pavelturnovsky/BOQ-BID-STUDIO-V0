# Přehled dat pro záložku Porovnání

Tento dokument shrnuje, jaká data se používají při generování záložky **Porovnání** v aplikaci BoQ Bid Studio a jaké hodnoty se zapisují do jednotlivých sloupců výsledné tabulky.

> **Důležité:** Záložka Porovnání pracuje s daty **jedna ku jedné** vůči původním sešitům. Jakékoli transformace (čištění, dopočty, agregace) se aplikují úplně stejně na master i dodavatele. V této části aplikace se záměrně neprovádí žádné další korekce, které by dodavatelská data upravovala odlišně od masteru.

## Zdrojová data

Porovnání pracuje s instancemi `WorkbookData`, které uchovávají pojmenovaný sešit a slovník listů s normalizovanými tabulkami a metadaty.

- Master sešit poskytuje referenční tabulky pro každý list, včetně sloupců s kódem položky, popisem, jednotkou, množstvím a cenami.
- Každý dodavatelský sešit je zpracován stejným způsobem; kromě tabulky může obsahovat i mapování sloupců.

## Zpracování master dat

1. Pro každý list se odstraní souhrnné řádky (např. mezisoučty) a prázdné popisy.
2. Zůstane základní sada sloupců: interní klíč `__key__`, případně `item_id`, `code`, `description`, `unit`, `quantity` a `total_price`. Pokud jsou k dispozici dílčí jednotkové ceny (`unit_price_material`, `unit_price_install`), vypočte se i kombinovaná jednotková cena.
3. Množství se převede na čísla a dopočítá se celková cena (`total_price`), pokud chyběla nebo byla nulová. Současně se připraví vážené průměry jednotkových cen podle klíče položky.
4. Řádky se agregují podle `__key__`. Výsledkem je referenční tabulka se sloupci `Master quantity`, `Master total` a případnými průměry jednotkových cen (`Master unit_price_material`, `Master unit_price_install`).

## Zpracování dat dodavatele

1. Načtená tabulka dodavatele projde **naprosto totožným** čištěním jako master (odstranění souhrnů, prázdných popisů, numerická konverze).
   U Porovnání je důležité neprovádět žádné dodatečné korekce, které by data měnila odlišně než u masteru – musí jít o transformace jedna ku jedné.
2. Určí se, které množstevní sloupce použít (`quantity_supplier` má přednost, jinak `quantity`) a které komponenty jednotkových cen obsahují data.
   Logika výběru a přepočtů opět zrcadlí master a nesmí dodavateli „pomáhat" vlastními úpravami.
3. Pokud je dostupná pouze celková cena, dopočte se jednotková cena z celkové ceny a množství. Naopak pokud chybí celková cena, dopočte se z jednotkové ceny a množství.
   Cílem je zachovat stejné hodnoty jako v původním souboru, pouze je zpřístupnit ve standardizované formě.
4. Data se agregují podle `__key__` a připraví se vážené průměry jednotkových cen dodavatele. K tomu se převezme první nenulová jednotka (`unit`).
5. Pro spojení s masterem se vybere vhodný join klíč (`item_id` má přednost, jinak kombinace kód+popis) a vytvoří se sloupec `__join_key__`.
6. Výsledná agregovaná tabulka dodavatele se sloučí s master tabulkou. Do výsledku se uloží sloupce `<Dodavatel> quantity`, `<Dodavatel> unit_price`, `<Dodavatel> total`, případně komponentní jednotkové ceny a jednotka.
7. Přidá se sloupec `<Dodavatel> Δ qty`, který udává rozdíl dodavatelského množství vůči master množství.
8. Pokud dodavatel obsahuje řádky, které se nepodařilo spárovat, přidá se extra řádek s popisem „Unmapped" a rozdílem v celkové ceně.

## Výpočty odchylek a metrik

- Pro každý dodavatelský sloupec `total` se uchovávají součty celkových cen pro kontrolu (`supplier_totals`).
- Atribut `master_total_sum` obsahuje celkovou hodnotu masteru pro daný list.

## Výstup pro záložku Porovnání

Finální `DataFrame` pro každý list tak obsahuje:

- referenční informace (`code`, `description`, `unit`, `Master quantity`, `Master total` a případné jednotkové ceny),
- sadu sloupců pro každého dodavatele (množství, jednotková cena, celková cena, rozdíly množství a případné komponentní ceny),
- metadata v atributech DataFrame pro následnou kontrolu (součty dodavatelů a součet masteru).

Tyto hodnoty se dále využívají při zobrazení tabulky v uživatelském rozhraní a pro výpočet procentuálních odchylek vůči master cenám.
