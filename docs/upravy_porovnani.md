# Úpravy aplikace podle zadání pro záložku „Porovnání"

Níže je stručný souhrn toho, jak byla aplikace BoQ Bid Studio přizpůsobena instrukcím z Kontroly dat, aby porovnání pracovalo přímo s načtenými tabulkami bez jakýchkoli přepočtů.

## 1. Zdroj dat
- Záložka Porovnání už nepoužívá žádné dodatečné datové transformace mimo ty, které vznikají při standardizaci tabulek v části Kontrola dat.
- Každý list masteru i dodavatelů se načte z objektů `WorkbookData`, které Kontrola dat vytvoří. V porovnání se tedy pracuje se stejnými daty, jaká vidí uživatel v kontrolní záložce.

## 2. Přenos hodnot „jedna ku jedné"
- Master tabulky se zpracují pouze odstraněním souhrnů a prázdných popisů; množství a ceny se přebírají přímo z tabulek bez přepočtu.
- U dodavatelů se kopírují ty samé sloupce (kód, popis, jednotka, množství, ceny). Pokud je k dispozici vlastní sloupec `quantity_supplier`, má prioritu, jinak se použije `quantity` beze změny hodnoty.
- Celkové i jednotkové ceny se pouze převádějí na numerickou podobu pro účely zobrazení – výsledek je identický s tím, co je v původním Excelu.

## 3. Párování položek
- Porovnání využívá interní klíč `__key__`, který Kontrola dat přiřazuje řádkům. Ten zaručuje, že master i dodavatelé jsou spárováni přesně podle mapování z kontrolní záložky.
- Není-li možné párovat podle `item_id`, použije se kombinace kódu a popisu, ale vždy jde o hodnoty z tabulek bez úprav.

## 4. Výstup v Porovnání
- Výsledná tabulka obsahuje sloupce `Master quantity`, `Master total` a jednotlivé sloupce pro každého dodavatele (množství, jednotková a celková cena). Tyto hodnoty jsou totožné s původními čísly.
- Aplikace dopočítává pouze odvozené metriky, které už byly v původním rozhraní (např. nejnižší cena, rozdíl vůči nejnižší nabídce). Tyto metriky nezasahují do základních dat, která byla přenesena z Kontroly dat.

## 5. Kontrola konzistence
- Do atributů tabulek se ukládají součty masteru i dodavatelů pro následné zobrazení kontrolních součtů. Opět jde o součty z převzatých hodnot, nikoli o nové výpočty.

Díky těmto úpravám odpovídá záložka Porovnání přímo datům z Kontroly dat a umožňuje pracovat s čísly v režimu „jedna ku jedné", jak vyžaduje zadání.
