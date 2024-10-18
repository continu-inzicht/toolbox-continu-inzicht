from datetime import datetime, timezone
import warnings
from pathlib import Path
from pydantic.dataclasses import dataclass
import pandas as pd
import urllib
import xarray as xr
from typing import Optional

from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class BelastingMatroos:
    """
    Belasting gegevens ophalen van rijkswaterstaat waterwebservices https://noos.matroos.rws.nl/
    """

    data_adapter: DataAdapter
    input: str
    output: str

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    url_retrieve_analysis: str = "https://noos.matroos.rws.nl/direct/get_anal_times.php?"
    url_retrieve_netcdf: str = "https://noos.matroos.rws.nl/direct/get_netcdf.php?"


    def generate_url(self, t_now, options, hindcast) -> str:
        """Returns the needed url to make the request to the Noos server

        Args:
            t_now: datetime
                Huidige tijd, wordt gebruikt om meest recente voorspelling op te halen
            options: dict
                opties die door gebruiker zijn opgegeven, in dit geval is source het belangrijkst
        """
        if "database" not in options:
            database = "maps1d"
        else:
            database = options["database"]
        datetime.strftime
        analysis = t_now.strftime("%Y%m%d%H%M")
        source = options["source"]
        url = (
            self.url_retrieve_netcdf
            + f"database={database}&"
            + f"source={source}&"
            + f"analysis={analysis}&"
            + f"hindcast={hindcast}&"
            + "timezone=GMT&"
            + "zip=0&"
        )

        return url

    
    
    
    ######## unused for now
    
    async def get_netcdf_file(self, url, temp_file):
        """
        Haal een netcdf bestand op gegeven de url en slaat die op in de tijdelijke map
        """
        file, res = urllib.request.urlretrieve(url, temp_file)
        return temp_file
    
    async def run_netcdf(self, input=None, output=None) -> None:
        """
        De runner van de Belasting WaterwebservicesRWS.
        """
        if input is None:
            input = self.input
        if output is None:
            output = self.output

        # haal opties en dataframe van de config
        global_variables = self.data_adapter.config.global_variables
        options = global_variables["BelastingMatroos"]

        self.df_in = self.data_adapter.input(input)

        # doe een data type check
        if "measuringstationid" not in self.df_in.columns:
            raise UserWarning(
                f"Input data missing 'measuringstationid' in columns {self.df_in.columns}"
            )
        wanted_measuringstationid = list(self.df_in["measuringstationid"].values)

        # zet tijd goed
        dt_now = datetime.now(timezone.utc)
        t_now = datetime(
            dt_now.year,
            dt_now.month,
            dt_now.day,
            dt_now.hour,
            0,
            0,
        ).replace(tzinfo=timezone.utc)

        # maak een url aan
        request_forecast_url = self.generate_url(t_now, options, hindcast=0)

        # in een tijdelijk map:
        # with tempfile.TemporaryDirectory() as tempdirname:
        tempdirname = Path.cwd()
        # haal het netcdf bestand op
        temp_file = (
            Path(tempdirname) / f"temp_netcdf_file_{t_now.strftime('%Y%m%d%H%M')}.nc"
        )
        f_name = await self.get_netcdf_file(request_forecast_url, temp_file)
        # change to open once formal, mf loads datset into memory
        try:
            ds_in = xr.open_dataset(f_name)

        # if the request is bad, handle this accordingly
        except ValueError:
            warnings.warn("No file found with matching results")
            dir = f_name.parent
            name = f_name.name.split(".")[0] + ".txt"
            f_name = await self.get_netcdf_file(request_forecast_url, dir / name)
            with f_name.open("r") as f_in:
                data = f_in.readlines()
            raise UserWarning(data)

        # een analyse tijd is voldoende
        ds = ds_in.isel(analysis_time=0)
        # namen van de stations staan apart opgeslagen
        station_names = [val.decode() for val in ds.node_id.values]
        # haal de ids die bij de station namen op:
        wanted_station_ids = [
            station_names.index(index) for index in wanted_measuringstationid
        ]
        ds = ds.sel(stations=wanted_station_ids)
        return ds

        parameters_in_vars = [
            parameter in ds.data_vars for parameter in options["parameters"]
        ]

        if all(parameters_in_vars):
            lst_parameters = []
            for parmeter in options["parameters"]:
                da_par = ds[parmeter]

                df = da_par.to_dataframe()
                lst_parameters.append(df)

            self.df_out = pd.concat(lst_parameters, axis=0)
            self.data_adapter.output(output=output, df=self.df_out)
            ds_in.close()

        else:
            warnings.warn(
                f"Parameter {options['parameters']} not found in database {options['database']}"
            )

