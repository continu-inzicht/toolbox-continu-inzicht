# from pathlib import Path
# from toolbox_continu_inzicht.base.config import Config


# def test_Config_csv():
#     """Tests loading of csv configuration"""
#     test_data_sets_path = Path(__file__).parent / "data_sets"
#     print(test_data_sets_path)
#     c = Config(config_path=test_data_sets_path / "test_config_csv.yaml")
#     c.lees_config()

#     assert "rootdir" in c.global_variables
#     keys = ["input", "output"]
#     assert all(key in c.WaardesKeerTwee for key in keys)
#     assert c.WaardesKeerTwee["input"]["type"] == "csv"


# def test_Config_dump():
#     test_data_sets_path = Path(__file__).parent / "data_sets"
#     print(test_data_sets_path)
#     c = Config(config_path=test_data_sets_path / "test_config_dump.yaml")
#     c.lees_config()

#     assert "FunctieDieErNietIs" in c.dump


# def test_Config_postgresql():
#     """Tests loading of postgresql configuration"""
#     test_data_sets_path = Path(__file__).parent / "data_sets"
#     print(test_data_sets_path)
#     c = Config(config_path=test_data_sets_path / "test_config_postgresql.yaml")
#     c.lees_config()

#     assert "rootdir" in c.global_variables
#     keys = ["input", "output"]
#     assert all(key in c.WaardesDelenTwee for key in keys)
#     assert c.WaardesDelenTwee["input"]["type"] == "postgresql_database"


# def test_Config_netCDF():
#     """Tests loading of netcdf configuration"""
#     test_data_sets_path = Path(__file__).parent / "data_sets"
#     print(test_data_sets_path)
#     c = Config(config_path=test_data_sets_path / "test_netcdf_config.yaml")
#     c.lees_config()

#     assert "rootdir" in c.global_variables
#     keys = ["input", "output"]
#     assert all(key in c.WaardesKeerTwee for key in keys)
#     assert c.WaardesKeerTwee["input"]["type"] == "netcdf"
