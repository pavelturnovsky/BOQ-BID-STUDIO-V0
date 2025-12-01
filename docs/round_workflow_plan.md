# Návrh logiky správy kol a práce se soubory

Aktualizovaný návrh reflektuje požadovaný tok „projekt → založit kolo → nahrát soubory → uložit/uzamknout“ a sjednocuje pojmenování akcí.

## Stavová mašina kola
- **draft** – kolo založeno, ale soubory nemusejí být kompletní; lze nahrávat/mazat/nahrazovat.
- **saved** – kolo uložené se všemi platnými vstupy; připraveno pro porovnávání.
- **locked** – kolo uzamčené, bez možnosti úprav; vhodné pro exporty a finální porovnání.

## Akce v UI
- **Založit kolo**
  - Vytvoří prázdné kolo v stavu `draft` a nastaví jej jako aktivní.
  - Připraví editovatelný název a poznámku (výchozí např. „Kolo 1“ + prázdná poznámka).
  - Vyčistí stav uploaderů a další cache, aby se nenatáhly „duchové“ z předchozího kola.
- **Uložit kolo**
  - Persistuje aktuální stav souborů + metadat aktivního kola a nastaví stav na `saved`.
  - Je dostupné pro draft i saved; po uložení refreshne UI, aby zobrazilo uložený stav.
- **Uzamknout kolo**
  - Přepne stav na `locked` a zakáže další nahrávání/upravy pro dané kolo.
  - Je dostupné po `saved` (nebo alespoň po draft s upozorněním, že uzamyká nekompletní data).

## Výchozí chování a prefill
- Prefill je **defaultně vypnutý**. Checkbox pouze umožní ruční natažení dat z posledního kola.
- Při přepnutí aktivního kola (selectbox) nebo po akci „Založit kolo“ se resetují všechny klíče uploaderů a metadata (`round_loaded_master`, `round_loaded_bids`, `supplier_metadata`, ad.).
- UI vždy jasně indikuje aktivní kolo a jeho stav, např. „Aktivní: Kolo X (draft/saved/locked)“.

## Tok práce
1. **Vytvoření projektu** automaticky založí první kolo v stavu draft se jménem „Kolo 1“ (editovatelným) a prázdnou poznámkou.
2. Uživatel **nahrává soubory** pro aktivní kolo (čisté prostředí, žádný prefill, pokud ho ručně nezapne).
3. **Uložit kolo** uloží vstupy + metadata a přepne stav na `saved`.
4. **Uzamknout kolo** po dokončení změn.
5. **Založit kolo** kdykoliv vytvoří nové prázdné kolo (`draft`), nastaví ho jako aktivní a vyčistí prostředí; uživatel nahrává soubory pro nové kolo.

## Multi-round porovnání
- Porovnávání pracuje primárně s koly ve stavech `saved` a `locked`; `draft` kola jsou z porovnání implicitně vyloučena.
- Pokud je potřeba zahrnout draft, UI by mělo upozornit, že jde o nekompletní podklady.

## Dopady na implementaci
- Uprav `project_storage.create_project` / `create_round` tak, aby uměly založit první kolo a vrátit `round_id` + defaultní název/poznámku.
- V session state drž `active_round_id`, `round_status`, `round_name`, `round_note`; při změně aktivního kola resetuj upload stavy a vypni prefill.
- Uploadery a formuláře svazuj s aktivním `round_id`; umožni uložit draft bez validních vstupů, ale přechod na `saved` validuj.
- Tlačítka pojmenuj lidsky: **Založit kolo**, **Uložit kolo**, **Uzamknout kolo**.
