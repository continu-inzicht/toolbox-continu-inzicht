from pathlib import Path
from continu_inzicht_toolbox import Config

# Voor nu  geen automatische tests .

if __name__ == "__main__":
    test_data_sets_path = Path.cwd() / "tests" / "src" / "data_sets"
    c = Config(config_path=test_data_sets_path / "test_config.yaml")
    c.read_config()
    print(c.waardes_delen_twee)
    print(c.waardes_keer_twee)
    print(c.dump)
