# Continu inzicht toolbox <img align="right" src="/docs/assets/logo.png" height="32" alt='logo'></img>

Python toolbox voor waterkeringbeheerders om risico's in kaart te brengen.

Voor documentatie zie: [https://continu-inzicht.github.io/toolbox-continu-inzicht/](https://continu-inzicht.github.io/toolbox-continu-inzicht/).

## Bijdragen

Zie het kopje [bijdragen](https://continu-inzicht.github.io/toolbox-continu-inzicht/contributing.html) op de wiki voor meer uitleg.

### Developen in het kort

#### Pixi

We maken gebruik van [pixi](https://pixi.sh/latest/) om de conda environment te beheren.

Om pixi te instaleren run je:

##### windows

```powershell
iwr -useb https://pixi.sh/install.ps1 | iex
```

##### Linux/Mac

```bash
curl -fsSL https://pixi.sh/install.sh | bash
```

#### Install pixi default environment

```bash
 cd ..../toolbox-continu-inzicht
 pixi install
```

#### Jupyter lab

```bash
 pixi run jupyter lab
```

of selecteer de environment `...\.pixi\envs\default\python.exe` in een notebook editor (VSCode).
