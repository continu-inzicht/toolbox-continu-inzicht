from pathlib import Path


def input_pastas_models(input_config: dict) -> dict[str, object]:
    """Laad Pastas-modellen uit een map met `.pas` bestanden.

    Parameters
    ----------
    input_config : dict
        DataAdapter-configuratie met minimaal `abs_path` naar de map met
        Pastas-modelbestanden.

    Returns
    -------
    dict[str, object]
        Dictionary met geladen Pastas-modellen, waarbij de sleutel gelijk is
        aan de bestandsnaam zonder extensie (bijv. `loc_a_01_tarso`) en de
        waarde een `pastas.model.Model` is.

    Raises
    ------
    UserWarning
        Als de map ontbreekt, geen map is, geen `.pas` bestanden bevat,
        of een modelbestand niet geladen kan worden.
    """
    import pastas as ps

    models_dir = Path(input_config["abs_path"])

    if not models_dir.exists():
        raise UserWarning(f"Pastas-modellenmap bestaat niet: {models_dir}")
    if not models_dir.is_dir():
        raise UserWarning(
            "Pastas-modellenpad moet naar een map verwijzen, geen bestand: "
            f"{models_dir}"
        )

    model_files = sorted(models_dir.glob("*.pas"))
    if len(model_files) == 0:
        raise UserWarning(
            f"Geen `.pas` modellen gevonden in map: {models_dir}. "
            "Voeg minimaal een modelbestand toe."
        )

    dict_models: dict[str, ps.model.Model] = {}
    for model_path in model_files:
        model_key = model_path.stem
        if model_key in dict_models:
            raise UserWarning(
                f"Dubbele modelnaam op basis van bestandsnaam gevonden: {model_key}"
            )
        try:
            dict_models[model_key] = ps.io.load(model_path)
        except Exception as exc:  # pragma: no cover - afhankelijk van Pastas errors
            raise UserWarning(
                f"Pastas-model kon niet worden geladen uit bestand: {model_path}"
            ) from exc

    return dict_models
