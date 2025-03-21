# Continu Inzicht Toolbox <img align="right" src="https://github.com/continu-inzicht/toolbox-continu-inzicht/raw/main/docs/assets/logo.png" height="40" alt='logo'/>

[![Run Pytest](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/tests.yml)
[![Render and Publish Docs](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/quarto-docs.yml/badge.svg)](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/quarto-docs.yml)
[![Pre-commit](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/pre-commit.yml)
[![Upload Python Package](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/python-publish.yml/badge.svg)](https://github.com/continu-inzicht/toolbox-continu-inzicht/actions/workflows/python-publish.yml)
[![PyPI](https://img.shields.io/pypi/v/toolbox-continu-inzicht)](https://pypi.org/project/toolbox-continu-inzicht)
[![image](https://img.shields.io/badge/fair--software.eu-%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8B-yellow)](https://fair-software.eu)

Python toolbox voor waterkeringbeheerders om risico's in kaart te brengen. Bij het ontwikkelen van de toolbox is rekening gehouden met type interacties met de toolbox: het gebruiken van de toolbox en bijdragen. Op de [wiki](https://continu-inzicht.github.io/toolbox-continu-inzicht/) is voor beide informatie beschikbaar.

## Gebruik van de toolbox

De toolbox kan geïnstalleerd worden met:

```bash
pip install toolbox-continu-inzicht
```

Het gebruik van een conda environment wordt sterk aanbevolen, hiervoor kan je gebruik maken van het volgende commando:

```bash
conda env create --file=https://raw.githubusercontent.com/continu-inzicht/toolbox-continu-inzicht/refs/heads/main/src/requirements.yaml
```

Onder het kopje [instaleren](https://continu-inzicht.github.io/toolbox-continu-inzicht/install.html) op de wiki vindt je meer informatie. Zie het kopje [modules](https://continu-inzicht.github.io/toolbox-continu-inzicht/modules.html) voor beschrijving van hoe je verschillende bouwstenen van de toolbox continu inzicht kan benutten, of bekijk het [voorbeeld](https://continu-inzicht.github.io/toolbox-continu-inzicht/examples/notebooks/proof_of_concept.html).

## Afhankelijkheden

Voor het berekenen van fragility curves worden twee packages gebruikt: [Pydra-core](https://github.com/HKV-products-services/pydra_core) voor GEKB en [probabilistic-piping](https://github.com/HKV-products-services/probabilistic_piping) voor STPH. Deze worden los ontwikkeld door HKV.

## License

Copyright (c) 2024 - 2025, HKV lijn in Water

[GPL 3.0 or later](https://github.com/continu-inzicht/toolbox-continu-inzicht/blob/main/LICENSE)
