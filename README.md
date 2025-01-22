# Toolbox Continu Inzicht  <img align="right" src="/docs/assets/logo.png" height="32" alt='logo'></img>

Python toolbox voor waterkeringbeheerders om risico's in kaart te brengen. Bij het ontwikkelen van de toolbox is rekening gehouden met type interacties met de toolbox: het gebruiken van de toolbox en bijdragen. Op de [wiki](https://continu-inzicht.github.io/toolbox-continu-inzicht/) is voor beide informatie beschikbaar.

## Gebruik van de toolbox

De toolbox kan _(straks)_ geïnstalleerd worden met: 

Bash:
```bash 
pip install toolbox-continu-inzicht
```

Onder het kopje [Installeren](https://continu-inzicht.github.io/toolbox-continu-inzicht/install.html) op de wiki vindt je meer informatie. Zie het kopje [Modules](https://continu-inzicht.github.io/toolbox-continu-inzicht/modules.html) voor beschrijving van hoe je verschillende bouwstenen van de toolbox continu inzicht kan benutten, of bekijk de [Voorbeelden](https://continu-inzicht.github.io/toolbox-continu-inzicht/examples/notebooks/proof_of_concept.html).

## Bijdragen aan de toolbox

Zie het kopje [Bijdragen](https://continu-inzicht.github.io/toolbox-continu-inzicht/contributing.html) op de wiki voor een uitgebreide uitleg.

### Developen in het kort

#### Pixi

We maken gebruik van [Pixi](https://pixi.sh/latest/) om de conda-environment te beheren.

Om Pixi te installeren run je:

##### Windows

Bash:
```bash
curl -fsSL https://pixi.sh/install.sh | bash
```

PowerShell:
```powershell
iwr -useb https://pixi.sh/install.ps1 | iex
```

##### Linux/Mac

Bash:
```bash
curl -fsSL https://pixi.sh/install.sh | bash
```

#### Installeer Python-omgeving met Pixi

Met het `pixi`-commando kun je vervolgens de juiste Python-bestanden installeren:

Bash:
```bash
 pixi install
```

Pixi gebruikt het `pixi.lock` bestand op de juiste packages te laden en zet deze in de `.pixi` map. Dit kan even duren.

#### Jupyter lab

Bash:
```bash
 pixi run jupyter lab
```

Of selecteer de juiste python instantie: `.pixi\envs\default\python.exe` in je ontwikkelomgeving. 
