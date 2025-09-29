# Návrh vylepšení záložky „Porovnání" a přehledů

Tento dokument shrnuje doporučení, jak rozvinout aplikaci BoQ Bid Studio tak, aby záložka **Porovnání** a související přehledy poskytovaly intuitivní, rychlou a „data-first" práci s nabídkami.

## 1. Architektura dat a sjednocení

1. **Centrální datový model**
   - Při načtení Master BoQ i nabídek vytvořit jednotnou datovou třídu (např. `ComparisonDataset`), která zajistí:
     - mapování sloupců na interní názvy,
     - uchování metadat (původní názvy listů, aliasy dodavatelů, použité sazby DPH/měny).
   - Výstupem bude „long-form" tabulka s identifikátorem položky, dodavatelem, jednotkovou i celkovou cenou, aby bylo možné data dále filtrovat a agregovat.

2. **Automatické přiřazení oddílů**
   - Rozšířit funkci `infer_section_group` tak, aby kombinovala heuristiku kódu s ontologií/konfigurací (JSON nebo YAML) definující přehled hlavních sekcí projektu.
   - Vložit do dat pole `section_id`, které umožní rychle filtrovat položky podle oddílu.

3. **Jednotné identifikátory položek**
   - `__join_key__` rozšířit o hash kombinující kód, popis, jednotku a případně další atributy. Umožní to přesnější párování napříč dodavateli.
   - Pro nelinkované řádky vést auditní tabulku s důvodem (např. chybějící kód, odlišná jednotka) a zobrazit ji ve speciálním panelu.

## 2. Uživatelské rozhraní a interaktivita

1. **Panel pro filtraci a vyhledávání**
   - V horní části záložky Porovnání přidat horizontální panel (Streamlit `st.container()` + `st.columns`) se vstupy:
     - vyhledávání podle kódu/popisu (`st.text_input` se `regex`/fuzzy vyhledáváním),
     - multi-select pro oddíly (`st.multiselect` napojený na `section_id`),
     - checkboxy pro zobrazení pouze položek s odchylkou nad/below prah,
     - volbu limitu výsledků (paging pomocí `st.dataframe` s `hide_index=True`).

2. **Interaktivní tabulky**
   - Použít `st.dataframe` s parametrem `use_container_width=True` a `column_config` pro nastavení formátů měn.
   - Implementovat vlastní funkci pro zvýraznění řádků (`st.dataframe` + `Styler`) podle hodnot v sloupci odchylky.
   - Přidat možnost stáhnout filtrovaná data jako CSV nebo XLSX (`st.download_button`).

3. **Detail položky**
   - Vedle hlavní tabulky zobrazit panel „Detail položky" (např. `st.expander`) zobrazující:
     - historii cen (Master vs. jednotliví dodavatelé),
     - graf průběhu (např. `plotly.express.bar`).
   - Kliknutí v tabulce zachytit přes `st.dataframe` + `selection_mode="single-row"` (Streamlit 1.31+) a zobrazit detail vybrané položky.

## 3. Práce s odchylkami a metrikami

1. **Konfigurovatelné prahy**
   - Do postranního panelu přidat `st.slider` pro nastavení horní/dolní procentní odchylky.
   - Vytvořit sloupce `Δ % vs Master` (kladná/ záporná hodnota) a pomocné bool sloupce `is_above_threshold`, `is_below_threshold`.

2. **Předdefinované pohledy**
   - Tlačítka (např. `st.radio`) pro rychlé filtry:
     - „Top 10 nejdražších odchylek" – setřídění dle absolutní odchylky,
     - „Nejnižší nabídky" – identifikace dodavatele s minimální cenou,
     - „Položky bez nabídky" – řádky, kde Master má hodnotu, ale dodavatel nikoliv.

3. **Souhrnné karty (KPIs)**
   - Nad tabulkou zobrazit 4 základní metriky (např. Streamlit `st.metric`):
     - počet položek nad prahem,
     - průměrná/mediánová odchylka,
     - celkový finanční dopad (součet absolutních rozdílů),
     - % položek bez kompletních dat.

## 4. AI asistent práce s daty

1. **Příprava embeddingů**
   - Při načtení dat vytvořit embedding pro popis položky (OpenAI text-embedding-3-large) a uložit jej do cache (`st.cache_data` / lokální SQLite).
   - Embeddding umožní:
     - semantické vyhledávání (uživatel napíše „podlahové topení" a zobrazí se relevantní položky),
     - shlukování podobných položek a návrh slučování duplicit.

2. **Chat asistent**
   - Integrovat konverzační panel (např. `st.chat_input`) napojený na GPT model, kterému předávat kontext:
     - agregované tabulky (limitované na N řádků),
     - metadata o prahových hodnotách.
   - Funkce asistenta:
     - vylistuje položky s nejvyššími odchylkami,
     - vysvětlí, proč je položka mimo rozsah (např. „Dodavatel A uvedl nulovou cenu, pravděpodobně chybí položka"),
     - navrhne skupiny kódu, které je vhodné zkontrolovat.

3. **Generování reportů**
   - Jedním tlačítkem vygenerovat souhrnný report (PDF/HTML) s komentářem AI shrnujícím:
     - top rozdíly,
     - položky bez nabídky,
     - doporučené kroky.
   - Report by čerpal ze stejného datového modelu jako UI, takže je konzistentní.

## 5. Výkonnost a UX

1. **Streaming načítání**
   - Využít `st.session_state` pro ukládání už nahraných souborů, aby se při přepínání listů znovu nenačítaly.
   - Pro velké tabulky zavést „virtuální scroll" (Streamlit 1.32) nebo manual paging.

2. **Logování a audit**
   - Každou akci (nastavení prahu, aplikace filtru) logovat do `st.session_state['audit_log']` a umožnit export.

3. **Validace dat**
   - Při nahrání upozornit na chybějící sloupce/řádky, zobrazit počet varování a umožnit jejich vylistování v panelu „Kontrola dat".

## 6. Technická roadmapa

1. **Fáze 1 – Datový základ a UI**
   - Refaktor načítání do jednotné datové vrstvy.
   - Přidat filtry, vyhledávání, KPI karty a detail položky.
   - Implementovat exporty a „Top N" pohledy.

2. **Fáze 2 – AI funkce**
   - Zavední embeddingů + semantické vyhledávání.
   - Chat asistent s rychlými příkazy (např. /odchylky, /nejnizsi).

3. **Fáze 3 – Automatizované reporty a notifikace**
   - Generování PDF/HTML reportů.
   - Možné propojení s e-mailem nebo Slackem (výstrahy při překročení limitů).

---

Doporučené změny zvýší přehlednost i analytickou sílu aplikace. Uživatel získá nástroje k rychlé identifikaci rizikových položek, může dynamicky pracovat s filtry a díky AI asistenci zrychlí kontrolu a komunikaci v rámci týmu.
