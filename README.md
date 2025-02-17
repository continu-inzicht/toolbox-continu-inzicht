# Continu Inzicht Toolbox <img align="right" src="/docs/assets/logo.png" height="32" alt='logo'></img>

Python toolbox voor waterkeringbeheerders om risico's in kaart te brengen. Bij het ontwikkelen van de toolbox is rekening gehouden met type interacties met de toolbox: het gebruiken van de toolbox en bijdragen. Op de [wiki](https://continu-inzicht.github.io/toolbox-continu-inzicht/) is voor beide informatie beschikbaar.

## Gebruik van de toolbox

De toolbox kan _(straks)_ geïnstalleerd worden met:

```bash
pip install toolbox-continu-inzicht
```

Tijdens het ontwikkelen is de package nog niet beschikbaar op PyPi, we raden aan om `Pixi` te gebruiken. Als je dat toch niet wilt kan je wel met pip installeren vanaf github met de volgende commandos:

```bash
pip install -e "git+https://github.com/continu-inzicht/toolbox-continu-inzicht@main#egg=toolbox_continu_inzicht&subdirectory=src"
```

onder het kopje [instaleren](https://continu-inzicht.github.io/toolbox-continu-inzicht/install.html) op de wiki vindt je meer informatie.
Zie het kopje [modules](https://continu-inzicht.github.io/toolbox-continu-inzicht/modules.html) voor beschrijving van hoe je verschillende bouwstenen van de toolbox continu inzicht kan benutten, of bekijk het [voorbeeld](https://continu-inzicht.github.io/toolbox-continu-inzicht/examples/notebooks/proof_of_concept.html).

## Bijdragen aan de toolbox

Zie het kopje [bijdragen](https://continu-inzicht.github.io/toolbox-continu-inzicht/contributing.html) op de wiki voor een uitgebreide uitleg.

### Developen in het kort

#### Pixi

We maken gebruik van [pixi](https://pixi.sh/latest/) om de conda environment te beheren.

<details>
    <summary>Installatie instructies Windows</summary>

```powershell
iwr -useb https://pixi.sh/install.ps1 | iex
```

</details>

<details>
    <summary>Installatie instructies Linux/Mac</summary>

```bash
curl -fsSL https://pixi.sh/install.sh | bash
```

</details>

#### Instaleer python packages met pixi

Met het `Pixi` commando in powershell kun je vervolgens de juiste python bestanden installeren:

```bash
 cd ..../toolbox-continu-inzicht
 pixi install
```

Dit kan even duren, Pixi gebruikt het `pixi.lock` bestand op de juiste packages te laden en zet deze in de `.pixi` map.

#### Jupyter lab

```bash
 pixi run jupyter lab
```

Of selecteer de juiste python instantie: `...\.pixi\envs\default\python.exe` in je ontwikkel omgeving.

#### Afhankelijkheden

Voor het berekenen van fragility curves worden twee packages gebruikt: [Pydra-core](https://github.com/HKV-products-services/pydra_core) voor GEKB en [probabilistic-piping](https://github.com/HKV-products-services/probabilistic_piping) voor STPH. Deze worden los ontwikkeld door HKV.

## License

Copyright (c) 2024 - 2025, HKV lijn in Water

GPL 3.0 or later
