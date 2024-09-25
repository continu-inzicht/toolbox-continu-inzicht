# Richtlijnen voor bijdragen  aan toolbox continu inzicht

In eerste instantie wordt de huidige functionaliteit, zoals origineel ontwikkeld door HKV, door een team van HKV in opdracht van een aantal waterschappen.
Hierbij is een belangrijk uitgangspunt dat de Python toolbox open source is, de ontwikkeling is dan ook voor iedereen te volgen.
Na de initiële  ontwikkeling door HKV, blijft het project open source en kan het op die manier worden door ontwikkeld.
De term 'ontwikkelaars' duidt in de eerste ontwikkel fase op het team van HKV, later op andere die een bijdrage willen leveren.
een bijdragen kan uit een lopen van een vraag tot een grote wijziging via een[pull
request](https://help.github.com/articles/about-pull-requests/).

Een bijdrage kan je een van de verschillende dingen zijn:

1. Je hebt een vraag
1. Je denkt een probleem (bug) te hebben gevonden (of onverwachte  functionaliteit)
1. Je wilt een aanpassing maken (Bug fixen, nieuwe functionaliteit, update aan de documentatie)
1. Je wilt een nieuwe versie publiceren

The sections below outline the steps in each case.

## Je hebt een vraag

1. Gebruik de zoek functie
    [hier](https://github.com/continu-inzicht/toolbox-continu-inzicht/issues) om te kijken
    of iemand anders de zelfde vraag heeft;
1. Als je niks vergelijkbaars kan vinden, maak een nieuwe issue aan.
1. Voeg de \"Question"\ label toe; voeg andere toe waar nodig.

## Je denkt een probleem (bug) te hebben gevonden

1. Gebruik de zoek functie
    [hier](https://github.com/continu-inzicht/toolbox-continu-inzicht/issues) om te kijken
    of iemand anders de zelfde vraag/probleem heeft;
1. Als je niks vergelijkbaars kan vinden, maak een nieuwe issue aan.
   Zorg dat je genoeg informatie mee geeft zodat andere ontwikkelaars je
   probleem begrijpen en genoeg context hebben om je te helpen.
   Afhankelijk van je probleem, kan je de [SHA
   hashcode](https://help.github.com/articles/autolinked-references-and-urls/#commit-shas) 
   van de commit die problemen veroorzaakt toevoegen. Denk daarnaast ook aan
   versie en besturingssysteem  informatie.

1. Voeg labels toe aan die relevant zijn voor je probleem.

## Je wilt een aanpassing maken

1. (**Belangrijk**) communiceer aan de rest van de ontwikkelaars dat
   *voor je begint*. Dit doe je via een issue.
1. (**Belangrijk**) Wacht tot er een overeenstemming is over je idee.
1. Indien nodig, maak een \'fork\' (kopie) in je eigen profiel. In deze fork
    maak je een eigen branch van de laatste  commit in main. Probeer om
    veranderingen die in de tussen tijd worden doorgevoerd op main al mee
    te nemen. dit doe je door te pullen van de \'upstream\'
    repository, (zie instructies
    [hier](https://help.github.com/articles/configuring-a-remote-for-a-fork/)
    en [hier](https://help.github.com/articles/syncing-a-fork/));
1. Als je [Visual Studio Code](https://code.visualstudio.com) gebruikt, staan er een aantal extensies aanbevolen.
1. Installeer de benodigde python packages in een pixi omgeving met `pixi install`, volg de uitleg van de [pixi](https://pixi.sh/latest/);
   Dit zorgt er voor dat iedereen de zelfde versies van python packages heeft.
1. Zorg dat alle types correct zijn met ``pixi run ruff check``.
1. Zorg dat alle bestaande testen werken met `pixi run pytest`;
1. Zorg dat alle documentatie automatisch genereert zonder erros met  `pixi run quarto-render`. [quarto](https://quarto.org/docs/computations/python.html) hier voor nodig, het behoort tot de `pixi` omgeving die is aangemaakt .
1. Voeg je eigen tests toe waar nodig;
1. Update en voeg documentatie toe. Gebruik [Numpy Style Python
    docstrings](https://numpydoc.readthedocs.io/en/latest/format.html#documenting-classes).
    Zorg dat je code leesbaar en begrijpelijk is voor andere.
1. [push](http://rogerdudler.github.io/git-guide/) je branch
    naar (jou fork van) de toolbox continu inzicht repo op GitHub;
1. Maak een pull request aan, bijvoobeeld volgens
    [deze instructies](https://help.github.com/articles/creating-a-pull-request/).

Als je het idee heb dat je iets nuttig heb toegevoegd,
maar weet niet hoe je test schrijft of runt,
of hoe je documentatie aanmaakt: geen probleem.
Maak een pull request en dan kijken we hoe we kunnen helpen.
Bij een pull request wordt altijd door een andere ontwikkelaar feedback gegeven.
Na het verwerken van deze feedback kan je aanpassing worden toegevoegd.

## Je wilt een nieuwe versie publiceren

Dit is een stukje voor de hoofd ontwikkelaar van toolbox continu inzicht.

1. Checkout ``HEAD`` van de ``main`` branch met ``git checkout main`` en ``git pull``.
1. Beslis welke nieuwe versie (major, minor or patch) gebruikt gaat worden. We gebruiken `semantic versioning <https://semver.org>`_.
1. Omdat je niet dircect naar main kan schrijven (protected), maak een nieuwe branch aan met ``git checkout -b release-<version>``.
1. Indien dependencies zijn aangepast, maak een nieuw [pixi lock](https://pixi.sh/latest/features/lockfile/) bestand.
1. Pas de versie aan in ``src/toolbox_continu_inzicht/__init__.py``, de ``pyptoject.toml`` leest deze uit.
1. Pas de ``Docs/changelog.qmd`` aan met de veranderingen. Vergeet de link naar de pull request niet. 
1. Zorg dat alle types correct zijn met ``pixi run ruff check``.
1. Zorg dat alle bestaande testen werken met `pixi run pytest`;
1. Commit & push je aanpassingen naar GitHub.
1. Maak een pull request aan, laat iemand het reviewen, wacht voor all actie, deze worden groen, en merge de pull request.
1. Wacht tot de GitHub acties op main klaar zijn en groen worden.
1. Maak een nieuwe \'release\' aan op GitHub
    - Gebruik de versie als title en pas een versie tag toe.
    - Als beschrijving gebruik de intro van de README.md en veranderingen uit de changelog.qmd.

1. Check

    1. Is de [wiki](https://continu-inzicht.github.io/toolbox-continu-inzicht/) upgedate?
    1. Heeft de Github actie alles naar [PyPI](https://pypi.org/project/continu_inzicht_toolbox) gestuurd?
    1. Werkt de nieuwe versie met:
        `pip3 install toolbox_continu_inzicht==<new version>`?

1. Vier je nieuwe versie!
