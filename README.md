# Continu inzicht toolbox <img align="right" src="/docs/assets/logo.png" height="32" alt='logo'></img>

Python toolbox voor waterkeringbeheerders om risico's in kaart te brengen.

Voor documentatie zie: [https://continu-inzicht.github.io/toolbox-continu-inzicht/](https://continu-inzicht.github.io/toolbox-continu-inzicht/).

## Bijdragen

Zie het kopje [bijdragen](https://continu-inzicht.github.io/toolbox-continu-inzicht/contributing.html) op de wiki voor meer uitleg.

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

Met Pixi kun je vervolgens de juiste python bestanden installeren:

```bash
 cd ..../toolbox-continu-inzicht
 pixi install
```

Dit kan even duren, Pixi gebruikt het `pixi.lock` bestand op de juiste packages te laden en zet deze in de `.pixi` map. 

#### Jupyter lab

```bash
 pixi run jupyter lab
```

Of selecteer `...\.pixi\envs\default\python.exe` in een notebook editor (bijv. Visual Studio Code).
