# Multi-user readiness plan

## Úkol 1: Přihlášení, session a identity
- Zajistit, že aplikace vykreslí hlavní UI pouze po úspěšném přihlášení; v opačném případě zobrazit login formulář se specifikovanými prvky.
- Navrhnout modul `auth` se záměnitelným rozhraním (`authenticate(username, password) -> User`), připravený na budoucí OIDC/SSO implementaci.
- User model musí obsahovat stabilní `user_id` (odlišné od `username`) pro bezpečné použití v cestách a metadatech a ukládá se jako `st.session_state["current_user"]`.
- Implementace `authenticate` nesmí ukládat hesla v plaintextu; konfigurace má držet hash (bcrypt/argon2) a při pilotní variantě je třeba doplnit kontrolu hesla proti hashi.
- Po přihlášení udržovat `current_user` v session a sledovat `last_activity` pro idle timeout; po expiraci vyžádat nové přihlášení.
- Přidat UI pro odhlášení (např. v sidebaru) s resetem `current_user` a navázaných session klíčů.

## Úkol 2: Per-user storage a autorizace
- Rozšířit konstruktory `OfferStorage` a `ProjectStorageManager` o povinný `user_id` a změnit kořen úložiště na `~/.boq_bid_studio/users/<user_id>/...`; všechny volající místa musí předávat `current_user.user_id`.
- Metadata projektů, kol i snapshotů doplnit o `schema_version`, `owner_user_id`, `created_by`, `last_modified_by` (kola/snapshoty alespoň `created_by`/`last_modified_by`); při změnách aktualizovat auditní pole.
- `user_id` je stabilní a nemělo by se měnit při úpravě `username`; projekty zůstávají navázané na původní identitu.
- Při načítání/ukládání validovat, že `current_user` je vlastník nebo má roli `admin`; při pokusu o cizí data vracet bezpečnou hlášku „Projekt nenalezen nebo k němu nemáte přístup.“ bez potvrzení existence.
- Metody pro listování mají vracet pouze data aktuálního uživatele; používat `pathlib` s fixními segmenty pro prevenci path traversal.
- Logovat klíčové akce (timestamp, user_id, project_id, round_id) pro auditovatelnost.
