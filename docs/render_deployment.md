# Nasazení BoQ Bid Studio na Render

Tento postup popisuje nasazení Streamlit aplikace BoQ Bid Studio na Render přes
Blueprint (`render.yaml`) a nastavení perzistentního disku pro uživatele,
projekty a nahrané soubory.

## 1. Nasazení přes Blueprint

1. Nahrajte repozitář na GitHub.
2. V Render otevřete **Blueprints** a zvolte **New Blueprint Instance**.
3. Vyberte GitHub repozitář aplikace.
4. Render načte soubor `render.yaml` z kořene repozitáře.
5. Potvrďte vytvoření služby `boq-bid-studio`.

Soubor `render.yaml` nastavuje:

- Python runtime,
- instalaci závislostí příkazem `pip install -r requirements.txt`,
- spuštění Streamlitu na adrese `0.0.0.0` a portu z proměnné `$PORT`, kterou
  Render doplní automaticky,
- healthcheck na cestě `/`,
- proměnnou `BOQ_STORAGE_DIR=/var/data/boq_bid_studio`,
- perzistentní disk připojený do `/var/data`.

Proměnnou `PORT` v Render ručně nenastavujte. Render ji poskytuje při spuštění
webové služby a aplikace ji používá ve startovacím příkazu.

## 2. Perzistentní úložiště

Aplikace ukládá data na disk. Bez perzistentního disku by se uživatelé,
projekty a nahrané soubory mohli ztratit při redeployi nebo restartu služby.
Blueprint proto přidává disk:

```yaml
disk:
  name: boq-bid-studio-data
  mountPath: /var/data
  sizeGB: 1
```

Aplikace používá proměnnou:

```text
BOQ_STORAGE_DIR=/var/data/boq_bid_studio
```

Pokud službu vytváříte ručně bez Blueprintu, nastavte stejnou proměnnou v
Render sekci **Environment** a přidejte disk v sekci **Disks** se stejným mount
path `/var/data`.

## 3. Ruční nastavení Web Service bez Blueprintu

Pokud nechcete použít Blueprint, vytvořte v Render novou **Web Service** a
nastavte:

| Položka | Hodnota |
| --- | --- |
| Runtime | `Python 3` |
| Build Command | `pip install -r requirements.txt` |
| Start Command | `streamlit run boq_bid_studio.py --server.address 0.0.0.0 --server.port $PORT --server.headless true` |
| Health Check Path | `/` |
| Environment Variable | `BOQ_STORAGE_DIR=/var/data/boq_bid_studio` |
| Disk Mount Path | `/var/data` |

## 4. Ověření po deployi

1. V Render otevřete službu a zkontrolujte, že poslední deploy skončil stavem
   **Live**.
2. V záložce **Logs** ověřte, že Streamlit naslouchá na portu z `$PORT`.
3. Otevřete veřejnou Render URL služby.
4. V aplikaci vytvořte testovací projekt, restartujte službu a ověřte, že
   projekt zůstal uložený. Tím potvrdíte správné připojení disku.

## 5. Nejčastější problémy

### Aplikace se nenačte nebo healthcheck selže

- Zkontrolujte, že start command obsahuje `--server.address 0.0.0.0`.
- Zkontrolujte, že port je `--server.port $PORT`, ne pevná hodnota jako `8501`.
- V logu hledejte chyby instalace balíčků z `requirements.txt`.

### Data po redeployi zmizela

- Ověřte, že služba má připojený disk do `/var/data`.
- Ověřte, že proměnná `BOQ_STORAGE_DIR` ukazuje na `/var/data/boq_bid_studio`.
- Pokud byla služba vytvořena bez disku, přidejte disk a znovu deployněte.

### Build selže na Python verzi

Blueprint nastavuje `PYTHON_VERSION=3.11.9`. Pokud Render nabízí novější
kompatibilní verzi, můžete ji změnit v `render.yaml` nebo v nastavení služby.
