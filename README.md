# Toolbox Continu Inzicht <img align="right" src="./docs/assets/logo.png" height="32" alt='logo'/>

[![Run Pytest](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/tests.yml)
[![Render and Publish Docs](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/quarto-docs.yml/badge.svg)](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/quarto-docs.yml)
[![Pre-commit](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/pre-commit.yml)
[![Upload Python Package](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/python-publish.yml/badge.svg)](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/python-publish.yml)
[![PyPI](https://img.shields.io/pypi/v/toolbox-continu-inzicht)](https://pypi.org/project/toolbox-continu-inzicht)
[![image](https://img.shields.io/badge/fair--software.eu-%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8B-yellow)](https://fair-software.eu)

De Toolbox Continu Inzicht is een Python-toolbox voor waterkeringbeheerders om risico's in kaart te brengen. Bij het ontwikkelen van de toolbox is rekening gehouden met de volgende type interacties met de toolbox: het gebruiken van de toolbox en bijdragen aan de toolbox. Op de [wiki](https://continu-inzicht.github.io/toolbox-continu-inzicht/) is over beide informatie beschikbaar.

## Gebruik van de toolbox

De toolbox kan geïnstalleerd worden met:

```bash
pip install toolbox-continu-inzicht
```

Het gebruik van een conda environment wordt sterk aanbevolen, hiervoor kan je gebruik maken van het volgende commando:

```bash
conda env create --file=https://raw.githubusercontent.com/continu-inzicht/toolbox-continu-inzicht/refs/heads/main/src/requirements.yaml
```

Onder [Installeren](https://continu-inzicht.github.io/toolbox-continu-inzicht/install.html) op de wiki vindt je meer informatie.
Zie [Modules](https://continu-inzicht.github.io/toolbox-continu-inzicht/modules_index.html) voor een beschrijving van de mogelijkheden van de bouwstenen van de Toolbox Continu Inzicht of bekijk de [voorbeelden](https://continu-inzicht.github.io/toolbox-continu-inzicht/voorbeelden.html).

## Bijdragen aan de toolbox

Zie [Bijdragen](https://continu-inzicht.github.io/toolbox-continu-inzicht/contributing.html) op de wiki voor een uitgebreide uitleg.

### Ontwikkelen in het kort

#### Pixi

We maken gebruik van [Pixi](https://pixi.sh/latest/) om de conda-environment te beheren.

<details>
<summary>Installatie-instructies Windows</summary>

```powershell
iwr -useb https://pixi.sh/install.ps1 | iex
```

</details>

<details>
<summary>Installatie-instructies Linux/Mac</summary>

```bash
curl -fsSL https://pixi.sh/install.sh | bash
```

</details>

#### Installeer Python-packages met Pixi

Met het `pixi` commando in PowerShell kun je vervolgens de juiste Python-packages installeren:

```bash
pixi install
```

Pixi gebruikt het `pixi.lock` bestand om de juiste packages te laden en zet deze in de `.pixi` map. Dit kan even duren.

#### JupyterLab

```bash
pixi run jupyter lab
```

Of selecteer de juiste Python-instantie: `..\.pixi\envs\default\python.exe` in je ontwikkelomgeving.
In VS Code kan dit met `Ctrl + shift + p` dan `Python: Select Interpreter`.  

#### Afhankelijkheden

Voor het berekenen van Fragility Curves worden twee packages gebruikt: [Pydra Core](https://github.com/HKV-products-services/pydra_core) voor GEKB en [Probabilistic piping](https://github.com/HKV-products-services/probabilistic_piping) voor STPH. Deze worden los ontwikkeld door HKV.

#### LFS

Voor een aantal bestanden wordt gebruik gemaakt van [Git LFS](https://git-lfs.com/). Zorg ervoor dat dit geïnstalleerd is om de repository goed te kunnen clonen.

Het gaat hierbij om kaarten als GeoTIFFs die groter zijn dan 1MB voor de voorbeelden en tests.

Gebruik bij het pushen van grote bestanden het volgende commando:

```bash
git lfs push origin {branchnaam} --all
```

## License

Copyright (c) 2024 - 2026, HKV lijn in Water

GPL 3.0 or later
