# Nasazení BoQ Bid Studio na Railway a propojení domény FORPSI

Tento návod používá jako hlavní adresu aplikace `https://www.boq-studio.cz`.
Kořenová doména `https://boq-studio.cz` se na ni přesměruje.

## 1. Nasazení služby na Railway

1. V Railway vytvořte projekt a propojte jej s GitHub repozitářem aplikace.
2. Railway použije soubor `railway.json`. Ten spustí Streamlit na adrese
   `0.0.0.0` a na portu z proměnné `$PORT`, kterou Railway doplní automaticky.
3. V **Service → Settings → Networking → Public Networking** zvolte
   **Generate Domain**.
4. Otevřete vygenerovanou adresu `*.up.railway.app` a ověřte, že aplikace
   funguje ještě před nastavováním vlastní domény.
5. Pokud deployment neprojde, zkontrolujte v Railway **Deploy Logs**, zda
   Streamlit nastartoval a neukončil se chybou.

Proměnnou `PORT` v Railway ručně nenastavujte, pokud k tomu nemáte zvláštní
důvod. Railway ji službě poskytuje a používá ji také pro healthcheck.

## 2. Trvalé úložiště

Aplikace ukládá uživatele, projekty a nahrané soubory na disk. Bez Railway
Volume by se tato data mohla při novém deploymentu ztratit.

1. V Railway přidejte ke službě **Volume**.
2. Nastavte mount path například na `/data`.
3. V **Service → Variables** přidejte:

   ```text
   BOQ_STORAGE_DIR=/data/boq_bid_studio
   ```

4. Proveďte redeploy.
5. Vytvořte testovací projekt, proveďte další redeploy a ověřte, že projekt
   zůstal dostupný.

Pro lokální spuštění není proměnná povinná. Aplikace nadále používá výchozí
adresář `~/.boq_bid_studio`.

## 3. Přidání `www.boq-studio.cz` v Railway

1. Otevřete **Service → Settings → Networking → Public Networking**.
2. Klikněte na **Custom Domain** a zadejte přesně:

   ```text
   www.boq-studio.cz
   ```

3. Railway zobrazí cílovou CNAME adresu, například
   `xxxxxxxx.up.railway.app`. Zkopírujte přesnou hodnotu z Railway; příklad
   z tohoto návodu nepoužívejte.
4. Doménu zatím v Railway nemažte. Po správném DNS nastavení ji Railway ověří
   a automaticky pro ni vystaví TLS/HTTPS certifikát.

## 4. Nastavení DNS ve FORPSI

V administraci FORPSI otevřete **Domény → boq-studio.cz → Editace DNS
záznamů**. Pokud tato volba není dostupná, doména používá jiné nameservery a
záznam je nutné upravit u jejich skutečného správce.

Pro subdoménu `www` nastavte:

| Název | Typ | Hodnota |
|---|---|---|
| `www` / `www.boq-studio.cz` | `CNAME` | přesná hodnota z Railway, např. `xxxxxxxx.up.railway.app` |

Při změně:

- odstraňte existující `A` nebo `AAAA` záznam pro **stejný název `www`**;
  CNAME nesmí být současně s jiným záznamem stejného názvu,
- cílovou Railway adresu zadejte bez `https://`, bez cesty a podle instrukcí
  FORPSI bez tečky na konci,
- nemažte MX/TXT záznamy používané pro e-mail,
- vyčkejte na propagaci DNS; Railway uvádí, že změna může celosvětově trvat
  až 72 hodin.

Správný výsledek lze ověřit například:

```bash
dig +short www.boq-studio.cz CNAME
curl -I https://www.boq-studio.cz
```

První příkaz má vrátit Railway CNAME a druhý platnou HTTP odpověď přes HTTPS.

## 5. Kořenová doména `boq-studio.cz`

Běžný CNAME nelze bezpečně použít na kořeni DNS zóny vedle povinných záznamů.
Nejjednodušší varianta při správě DNS u FORPSI je proto:

1. ponechat aplikaci na `www.boq-studio.cz`,
2. u FORPSI objednat/nastavit službu **Redirect**,
3. nastavit trvalé přesměrování HTTP 301 z `boq-studio.cz` na:

   ```text
   https://www.boq-studio.cz
   ```

Pokud nechcete používat placený Redirect od FORPSI, lze DNS přesunout ke
správci podporujícímu `ALIAS`/CNAME flattening pro kořenovou doménu a přidat
v Railway také `boq-studio.cz`. Při přesunu DNS je nutné přenést i všechny
MX, TXT a další používané záznamy.

## 6. Diagnostika

Postupujte vždy v tomto pořadí:

1. Deployment v Railway je zelený.
2. Funguje vygenerovaná Railway adresa `*.up.railway.app`.
3. `dig +short www.boq-studio.cz CNAME` vrací hodnotu přidělenou Railway.
4. Railway ukazuje zelené ověření u přesné domény `www.boq-studio.cz`.
5. Funguje `https://www.boq-studio.cz`.
6. Nakonec funguje přesměrování z `https://boq-studio.cz`.

Pokud nefunguje už krok 2, nejde o problém DNS/FORPSI. Zkontrolujte Railway
Deploy Logs. Pokud funguje krok 2, ale ne krok 3, je problém v DNS nastavení.
Pokud funguje krok 3 a Railway doménu ověřilo, ale HTTPS ještě nefunguje,
vyčkejte na vystavení certifikátu a propagaci DNS.

## Oficiální návody

- Railway: <https://docs.railway.com/deploy/exposing-your-app>
- Railway healthchecks a proměnná `PORT`: <https://docs.railway.com/reference/healthchecks>
- FORPSI nastavení DNS: <https://support.forpsi.com/kb/a4183/nastaveni-dns-zaznamu.aspx>
- FORPSI Redirect: <https://support.forpsi.com/kb/a4680/redirect-presmerovani-domeny.aspx>
