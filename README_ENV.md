# README_ENV

Ce dépôt utilise `.env.atlas` comme gabarit filtré (clés USED uniquement) pour générer vos `.env` locaux.

## Étapes rapides
1. Copier le gabarit : `cp .env.atlas .env`.
2. Mettre les secrets sensibles dans `.env.secrets` (mêmes clés, valeurs réelles).
3. Charger l'environnement avant d'exécuter : `source .env && source .env.secrets && python boot.py`.
4. Les structures JSON doivent être encadrées par des quotes simples (ex: `KEY='{"a":1}'`).
5. Les listes CSV n'ont pas de crochets (ex: `A,B,C`). Utilisez JSON list si vous avez besoin de types bool/numériques (ex: `KEY='[1,0,1]'`).
6. Booléens acceptés : `1/0` ou `true/false` (sans guillemets).
7. Erreurs fréquentes : doubles guillemets autour du JSON, espaces après les virgules, duplication de clés entre `.env` et `.env.secrets`.

## Exemples rapides
- Liste CSV : `ENABLED_EXCHANGES=BINANCE,BYBIT,COINBASE`
- Liste JSON : `ALLOWED_ROUTES='[["BINANCE","BYBIT"],["BYBIT","BINANCE"]]'`
- Dict JSON : `PWS_REGION_MAP='{"BINANCE":"EU","BYBIT":"EU","COINBASE":"US"}'`
- Bool : `DISCOVERY_ENABLED=0`

## Comptages
- USED incluses : 540
- TEST_ONLY : 3 (commentées en fin de .env.atlas)
- NO_USAGE_FOUND exclues : 98

## Rappel
Ne jamais commiter de secrets réels. Remplacez les placeholders `__REPLACE_ME__` dans votre `.env.secrets` privé.
