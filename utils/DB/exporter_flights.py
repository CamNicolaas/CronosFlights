import asyncio, dotenv, os, boto3
from botocore.client import BaseClient
import botocore.exceptions
from datetime import datetime
from urllib.parse import urlparse
import pandas as pd
from pathlib import Path
from typing import Optional, Union, Any, Dict, List
from github import Auth
from github import Github
from github.GithubException import UnknownObjectException

from utils.DB import AsyncFlightDBManager
from utils import (
    AsyncMessageHandler,
    NotifyDiscord,
    AsyncConfigManager,
    
    # --- Exceptions ---
    RepositoryNotFoundError,
    UploadToGithubError,
    SaveDataToParquetError,
    ExporterFlightsDataError,
    CheckerS3ExistBucketError,
    UploadFileToBucketError,
    CheckerS3ExistFolderError,
    UploadToAWSS3Error
)



class ExporterFlightsData:
    def __init__(self,
        log_name:Optional[str] = "ExporterFlightsData", 
        log_path:Optional[Union[str, Path]] = "./aerolineasARG/Tools"
    ):
        # --- Configs ---
        self.configs:Dict[str, Dict[str, Any]] = dict()
        # --- Github Auth ---
        self.github_auth:Github = None
        # --- AWS Auth ---
        self.aws_s3:BaseClient = None
        # --- Notify System ---
        self.notifyer = NotifyDiscord()
        # --- DB Flights ---
        self.db_flights = AsyncFlightDBManager()
        # --- Logger ---
        self.logger = AsyncMessageHandler(
            log_filename = log_name,
            logs_folder = log_path,
            printer_msg = "[AerolineasArg][Exporter Flights Data] Status:"
        )
    

    async def load_configs(self) -> None:
        try:
            await self.db_flights.connect_db()
            env_values = dotenv.dotenv_values()
            configs = AsyncConfigManager()
            self.configs = {
                "admin": await configs.get_configs("monitor_configs", "admin_configs", "webhooks"),
                "general": await configs.get_configs("monitor_configs", "flights", "aerolineas_argentinas"),
                "aws_s3_uri": env_values["S3_URI"]
            }
            # --- Auth Github ---
            self.github_auth = Github(
                auth = Auth.Token(env_values["GITHUB_ACCESS"])
            )
            # --- Auth AWS S3 ---
            self.aws_s3 = boto3.client(
                's3',
                aws_access_key_id = env_values["S3_ACCESS_KEY"],
                aws_secret_access_key = env_values["S3_SECRET_KEY"],
                region_name = env_values["AWS_REGION"]
            )

        except Exception as err:
            raise ExporterFlightsDataError(
                f'Error, Could Not Load Configuration...',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )


# ---------- Main Method. ----------
    async def main_process(self):
        try:
            await self.load_configs()
            
            path_files_list = await self.export_data()
            date = datetime.now()
            for file in path_files_list:
                #I think its would be a good idea update to make modular with other providers
                if file["aws_s3"]:
                    await self.upload_to_aws_s3(
                        local_path = file["path"],
                        date = date
                    )
                if file["github"]:
                    await self.upload_to_github(
                        local_path = file["path"],
                        remote_path = f'FlightsData/AerolineasARG/{Path(file["path"]).name}', 
                        remote_repo_name = "CronosFlights"
                    )

        except Exception as err:
            message_error = f'Error Fatal, Kill Process | Type: {type(err).__name__} | Message: {str(err)}'
            # --- Send Status to Discord ---
            _, response_webhook = await self.notifyer.error_admin(
                URL_Webhook = self.configs["admin"]["status_monitors"],
                store_data = self.configs["general"],
                problem_logs = f'[AerolineasArg][Exporter Flights Data] Status: {message_error}'
            )
            # --- Save Log ---
            await self.logger.error(
                message = message_error,
                hidden_msg = response_webhook
            )
        finally:
            await self.db_flights.disconnect_db()
            await self.logger.shutdown()
            # --- PyGithub Close Conn ---
            self.github_auth.close()


# ---------- Proccess & Export Data ----------
    # --- Prepair Flights Data ---
    async def export_data(self) -> List[Dict[str, object]]:
        table_config = {
            "flight_sections": {
                "github": True,
                "aws_s3": True
            },
            "flights_calendar": {
                "github": True,
                "aws_s3": True
            },
            "price_stats": {
                "github": True,
                "aws_s3": True
            },
            "price_history_calendar": {
                "github": False,
                "aws_s3": True
            }
            #"airports": {
            #    "github": False,
            #    "aws_s3": False
            #},
        }

        path_files_list = list()
        for table, config in table_config.items():
            await self.logger.info(f'Start the table export process: {table}')
            
            rows = await self.db_flights.export_table_data(
                table_name = table
            )
            path_file = self._save_data_parquet(
                filename = table,
                table_rows = rows
            )
            await self.logger.info(f'The Table: "{table}" Was Exported Successfully')

            path_files_list.append({
                "path": path_file,
                "github": config["github"],
                "aws_s3": config["aws_s3"]
            })

        return path_files_list

    # --- Save pd to .parquet ---
    def _save_data_parquet (self,
        filename:str, 
        table_rows:List[Dict], 
        path_folder:str = "FlightsData/AerolineasARG"
    ) -> str:
        
        try:
            os.makedirs(path_folder, exist_ok = True)

            file_path = Path(path_folder) / f"{filename}.parquet"
            df = pd.DataFrame(table_rows)
            df.to_parquet(file_path, index = False)
            return str(Path(file_path).resolve())

        except Exception as err:
            raise SaveDataToParquetError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

    # --- Upload Data to Github ---
    async def upload_to_github(self,
        local_path:Union[str, Path],
        remote_path:str,
        remote_repo_name:str, # = "CronosFlights",
        branch:str = "main"
    ) -> None:
        
        try:
            await self.logger.info(f'Starting File Upload Process To GitHub')

            for repo in self.github_auth.get_user().get_repos():
                if remote_repo_name in repo.full_name:
                    repo_obj = self.github_auth.get_repo(repo.full_name)
                    file_name = Path(local_path).name

                    with open(local_path, "rb") as file:
                        file_content = file.read()

                    try:
                        check_repo_exists = repo_obj.get_contents(remote_path, ref = branch)

                        repo_obj.update_file(
                            path = remote_path,
                            message = f"Update: Flight Data File: {file_name}",
                            content = file_content,
                            sha = check_repo_exists.sha,
                            branch = branch
                        )
                        await self.logger.success(f'Upload Success!! File: {file_name} Has Been Successfully Updated In Repository {repo.full_name}')

                    except UnknownObjectException:
                        await self.logger.critical(f'New File: {file_name} Will Be Uploaded To Repository: {repo.full_name}')

                        repo_obj.create_file(
                            path = remote_path,
                            message = f'New: Add New Flight Data File: {file_name}',
                            content = file_content,
                            branch = branch
                        )
                        await self.logger.success(f'Upload Success!! New File: {file_name} Successfully Uploaded To Repository: {repo.full_name}')
                    return
            
            raise RepositoryNotFoundError(
                f'Error, Repository "{remote_repo_name}" Was Not Found...',
                context = {
                    "error_type": "RepositoryNotFoundError",
                    "error_msg": f'Repository Not Found: "{remote_repo_name}"'
                }
            )
        except Exception as err:
            raise UploadToGithubError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )


    # --- Upload to AWS S3 ---
    async def upload_to_aws_s3(self,
        local_path:Union[str, Path],
        date:datetime
    ) -> None:
        
        try:
            file_name = Path(local_path).name
            await self.logger.info(f'Starting File: {file_name} Upload Process To AWS S3')

            bucket, folder = type(self).parse_s3_uri(self.configs["aws_s3_uri"])
            # --- Checker Bucket ---
            self._checker_exist_bucket(bucketname = bucket)
            # --- Checker Folder ---
            self._checker_exist_folder(
                bucketname = bucket, 
                foldername = folder
            )
            file_key = self.build_s3_key(
                filename = file_name,
                foldername = folder,
                dt = date
            )
            # --- Upload Files ---
            self._upload_file_bucket(
                path_file = local_path,
                bucketname = bucket,
                key = file_key
            )
            await self.logger.success(f'Upload successful! File: {file_name} Was Successfully Uploaded To Bucket.')

        except Exception as err:
            raise UploadToAWSS3Error(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )
        

    # --- Check If Bucket Exists ---
    def _checker_exist_bucket(self,
        bucketname:str
    ) -> bool:
        
        try:
            self.aws_s3.head_bucket(Bucket = bucketname)
            return True
        
        except botocore.exceptions.ClientError as err:
            raise CheckerS3ExistBucketError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )
    
    # --- Check If Folder Exists ---
    def _checker_exist_folder(self,
        bucketname:str,
        foldername:str
    ) -> bool:

        try:
            response = self.aws_s3.list_objects_v2(
                Bucket = bucketname, 
                Prefix = foldername, 
                MaxKeys = 1
            )
            if not 'Contents' in response:
                raise CheckerS3ExistFolderError(
                    f'Error The Bucket Are Looking Does Not Exist, Check Configs.',
                    context = {
                        "error_type": "CheckerS3ExistFolderError",
                        "error_msg": "Bucket Are Search Does Not Exist..."
                    }
                )
            return True
        
        except botocore.exceptions.ClientError as err:
            raise CheckerS3ExistFolderError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

    # --- Upload File to Bucket ---
    def _upload_file_bucket(self,
        path_file:str,
        bucketname:str,
        key:str
    ) -> bool:
        
        try:
            self.aws_s3.upload_file(
                Filename = path_file,
                Bucket = bucketname,
                Key = key
            )
            return True
        
        except botocore.exceptions.ClientError as err:
            raise UploadFileToBucketError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )



# ---------- Tools ----------
    @staticmethod
    def parse_s3_uri(s3_uri:str):
        if not s3_uri.startswith("s3://"):
            raise ValueError("The S3 Instance URI Not Appear In Correct Format.")

        parsed = urlparse(s3_uri)
        bucket = parsed.netloc
        folder = parsed.path.lstrip('/').rstrip('/')
        return bucket, folder

    @staticmethod
    def build_s3_key(
        filename:str, 
        foldername:str, 
        dt:datetime
    ) -> str:

        return f'{foldername}/dynamic-data/year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}/minute={dt.minute:02d}/{filename}'


if __name__ == "__main__":
    async def main():
        exporters = ExporterFlightsData()
        await exporters.main_process()

    asyncio.run(main())
