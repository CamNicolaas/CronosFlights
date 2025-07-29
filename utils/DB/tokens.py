
import dotenv
from datetime import datetime
from pymongo import ASCENDING
from pymongo.errors import PyMongoError
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional, Dict, Tuple, List

from utils.tools import SingletonClass
from utils.exceptions import (    
    DBCannotConnectError,
    CannotGetTokenError,
    CannotSetTokenError,
    CannotUpdateTokenError,
    CannotDeleteTokenError,
    InexistTokenError,
    DBTokensError
)



class DBTokensManager(metaclass = SingletonClass):
    def __init__(self):
        self._configs = dotenv.dotenv_values()
        self._mongo: Optional[AsyncIOMotorClient] = None
        self._collection = None
        self._message:str = f'[DB Tokens Manager]'


# ---------- Methods Manager ----------
    # --- Connect Tokens DB ---
    async def connect_db(self, 
        website:str = "AerolineasArg"
    ) -> Tuple[bool, str]:
        
        try:
            self._mongo = AsyncIOMotorClient(
                self._configs["DB_MONGO"]
            )
            self._collection = self._mongo["Tokens"][website]
            await self._collection.create_index([("active", ASCENDING)])
            return True, f'{self._message} Database Connected | Collection: {website}'

        except PyMongoError as err:
            raise DBCannotConnectError(
                f'{self._message} Error During Connection...',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

    # --- Disconnect Tokens DB ---
    async def disconnect_db(self) -> Tuple[bool, str]:
        try:
            if self._mongo:
                self._mongo.close()
                self._mongo = None
                self._collection = None
                return True, f'{self._message} Database Disconnected Successfully'
            return False, f'{self._message} No Active Connection To Disconnect'
        
        except Exception as err:
            raise DBTokensError(
                f'{self._message} Error During Disconnection...',
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

    # --- Save New Token ---
    async def set_token(self, 
        bearer_token:str
    ) -> Tuple[bool, str]:
        
        try:
            await self._collection.insert_one({
                "active": True,
                "bearer_token": bearer_token,
                "inserted_at": datetime.now()
            })
            return True, f'{self._message} Token Saved Successfully'
        
        except PyMongoError as err:
            raise CannotSetTokenError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )
    
    # --- Return Tokens List ---
    async def get_tokens(self,
        only_active:bool = True
    ) -> Tuple[bool, List[Dict]]:
    
        try:
            tokens = await self._collection.find(
                {"active": True} if only_active else {}
            ).to_list(length = None)

            return bool(tokens), tokens
        
        except PyMongoError as err:
            raise CannotGetTokenError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

    # --- Update Specific Token ---
    async def update_token(self, 
        bearer_token:str
    ) -> Tuple[bool, str]:
        
        try:
            result = await self._collection.update_one(
                {"bearer_token": bearer_token},
                {"$set": {"active": False}}
            )
            if result.modified_count == 1:
                return True, f'{self._message} Token Marked As Inactive: {bearer_token[-25:]}'
            return False, f'{self._message} Token: ...{bearer_token[-25:]} Not Found, Cannot Be Updated.'
        
        except PyMongoError as err:
            raise CannotUpdateTokenError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

    # --- Delete Specific Token ---
    async def delete_token(self, 
        bearer_token:str
    ) -> Tuple[bool, str]:

        try:
            result = await self._collection.delete_one(
                {"bearer_token": bearer_token}
            )
            if result.deleted_count == 1:
                return True, f'{self._message} Token ...{bearer_token[-25:]} Deleted Successfully'
            return False, f'{self._message} Token ...{bearer_token[-25:]} Not Found, Cannot Be Deleted'
        
        except PyMongoError as err:
            raise CannotDeleteTokenError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

    # --- Check If Token Exist ---
    async def check_token_exists(self,
        bearer_token:str
    ) -> Tuple[bool, str]:

        try:
            token = await self._collection.find_one(
                {"bearer_token": bearer_token}
            )
            return bool(token), f'{self._message} Token Exists: ...{bearer_token[-25:]}' if token else f'{self._message} Token ...{bearer_token[-25:]} Not Found'
        
        except PyMongoError as err:
            raise InexistTokenError(
                context = {
                    "error_type": type(err).__name__,
                    "error_msg": str(err)
                }
            )

