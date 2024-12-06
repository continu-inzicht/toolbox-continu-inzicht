# Continu Inzicht Toolbox <img align="right" src="/docs/assets/logo.png" height="32" alt='logo'></img>

Python toolbox voor waterkeringbeheerders om risico's in kaart te brengen. Bij het ontwikkelen van de toolbox is rekening gehouden met type interacties met de toolbox: het gebruiken van de toolbox en bijdragen. Op de [wiki](https://continu-inzicht.github.io/toolbox-continu-inzicht/) is voor beide informatie beschikbaar.

## Gebruik van de toolbox

De toolbox kan _(straks)_ geinstaleerd worden met:

```bash
pip install toolbox-continu-inzicht
```

onder het kopje [instaleren](https://continu-inzicht.github.io/toolbox-continu-inzicht/install.html) op de wiki vindt je meer informatie.
Zie het kopje [modules](https://continu-inzicht.github.io/toolbox-continu-inzicht/modules.html) voor beschrijving van hoe je verschillende bouwstenen van de toolbox continu inzicht kan benutten, of bekijk het [voorbeeld](https://continu-inzicht.github.io/toolbox-continu-inzicht/examples/notebooks/proof_of_concept.html).

## Bijdragen aan de toolbox

Zie het kopje [bijdragen](https://continu-inzicht.github.io/toolbox-continu-inzicht/contributing.html) op de wiki voor een uitgebreide uitleg.

### Developen in het kort

#### Pixi

We maken gebruik van [pixi](https://pixi.sh/latest/) om de conda environment te beheren.

Om Pixi te installeren run je:

##### windows

```powershell
iwr -useb https://pixi.sh/install.ps1 | iex
```

##### Linux/Mac

```bash
curl -fsSL https://pixi.sh/install.sh | bash
```

Hiermee installeer je het programma pixi.

#### Instaleer python omgeving met pixi

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
