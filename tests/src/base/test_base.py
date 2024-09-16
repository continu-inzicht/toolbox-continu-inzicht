from pathlib import Path
from continu_inzicht_toolbox.base import Config, DataAdapter

# Voor nu  geen automatische tests .

if __name__ == "__main__":
    test_data_sets_path = Path.cwd() / "tests" / "src" / "data_sets"
    c = Config(config_path=test_data_sets_path / "test_config.yaml")
    c.lees_config()
    d = DataAdapter(config=c)
    print(d.input("waardes_keer_twee"))
