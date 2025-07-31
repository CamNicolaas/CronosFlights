from typing import Optional


class CronosFlightsExceptions(Exception):
    gen_message: str = "An Unknown Error Occurred..."

    def __init__(
        self,
        message: Optional[str] = None,
        *,
        context: Optional[dict] = None
    ):
        self.message = message or self.gen_message
        self.context = context or {}
        super().__init__(self.message)

    def __str__(self):
        base = self.message
        if self.context:
            base += f' | Context: {self.context}'
        return base


# ---------- Scraper Exceptions ----------
class ScrapersError(CronosFlightsExceptions):
    gen_message = "General Error Related To Scraper..."

class UnauthorizedTokenAPI(ScrapersError):
    gen_message = "Error, The 'Bearer token' Is Not Valid..."
class ExpiredTokenAPI(ScrapersError):
    gen_message = "Error, The 'Bearer Token' Has Expired, Please Try To Retrieve a New..."
class InvalidPastDate(ScrapersError):
    gen_message = "Error, Check The Date, Not Be Earlier Than Today..."
class InvalidFormatDate(ScrapersError):
    gen_message = "Error, Check The Date, Format Is Invalid..."
class InvalidRequests(ScrapersError):
    gen_message = "Error, API May Be Down Or Unable To Process This Request..."
class CannotGetBearerTokenError(ScrapersError):
    gen_message = "Error, Cannot Retrieve Token Error, Check HTML Body..."
class UnknownError(ScrapersError):
    gen_message = "Fatal Error Unknown!, The API Returned Unhandled Error..."
class RequestsBlocked(ScrapersError):
    gen_message = "Error, Site Blocking Access API Resources..."
class GdsResponseError(ScrapersError):
    gen_message = "Error, GDS Response Invalid..."
class SiteServiceDown(ScrapersError):
    gen_message = "Error, API Currently Out Of Service..."


# ---------- MongoDB Exceptions ----------
class DBTokensError(CronosFlightsExceptions):
    gen_message = "General Error Related to DBTokensManager..."

class DBCannotConnectError(DBTokensError):
    gen_message = "Error Could Not Connect To Database..."
class CannotGetTokenError(DBTokensError):
    gen_message = "Error Could Not Get Tokens..."
class TokenAlreadyExists(DBTokensError):
    gen_message = "Error, This Token Already Exists In The Data Base..."
class CannotSetTokenError(DBTokensError):
    gen_message = "Error Token Could Not Be Saved To The Database..."
class CannotUpdateTokenError(DBTokensError):
    gen_message = "Error Trying Update Token Bearer..."
class CannotDeleteTokenError(DBTokensError):
    gen_message = "Error Token Could Not Be Deleted..."
class InexistTokenError(DBTokensError):
    gen_message = "Error Requested Token Does Not Exist..."


# ---------- Postgres Exceptions ----------
class DBFlightsError(CronosFlightsExceptions):
    gen_message = "General Error Related to DBFlightsManager..."

class DBFlightsConnectionError(DBFlightsError):
    gen_message = "Error Could Not Connect To Flights Database..."
class DBFlightsDisconnectError(DBFlightsError):
    gen_message = "Error Could Not Disconnect From Flight Database..."
class DBFlightCheckerError(DBFlightsError):
    gen_message = "Error Could Flight Not Be Queried By Hash..."
class DBFlightsAirportsError(DBFlightsError):
    gen_message = "Error Could Not Recover IATA Airports..."
class DBFlightsFindNotifysError(DBFlightsError):
    gen_message = "Error Finding Sent Notifications..."
class DBFlightsMarkNotifysError(DBFlightsError):
    gen_message = "Error Marking Notification as Sent..."
class DBFlightsNewFlightError(DBFlightsError):
    gen_message = "Error Adding New Flight To Database..."
class DBFlightsUpdateFlightError(DBFlightsError):
    gen_message = "Error Updating Flight To Database..."
class ExportTableFlightError(DBFlightsError):
    gen_message = "Error Cannot Export Table..."

# ---------- Kafka Exceptions ----------
class KafkaManagerError(CronosFlightsExceptions):
    gen_message = "General Error Related To KafkaConsumer..."

class KafkaCannotConnectError(KafkaManagerError):
    gen_message = "Could Not Connect Broker..."
class KafkaCannotDisconnectError(KafkaManagerError):
    gen_message = "Could Not Disconnect Broker..."
class KafkaCreaterTopicsError(KafkaManagerError):
    gen_message = "Error Occurred While Generating Topics...."
class KafkaProducerError(KafkaManagerError):
    gen_message = "Error Occurred While Sending Message..."
class KafkaConsumerError(KafkaManagerError):
    gen_message = "Error Occurred While Processing Message..."


# ---------- Deals Exceptions ----------
class DealsAnalyzerManager(CronosFlightsExceptions):
    gen_message = "General Error Related To DealsAnalyzer..."

class GenerateIQRError(DealsAnalyzerManager):
    gen_message = "Error Occurred While Calculate New IQR..."
class ReturnIQRError(DealsAnalyzerManager):
    gen_message = "Error Occurred While Return IQR...."
class ReturnMinMonthlyError(DealsAnalyzerManager):
    gen_message = "Error Occurred While Getting IQR...."
class ReturnMinDailyError(DealsAnalyzerManager):
    gen_message = "Error Occurred While Returning Daily Min..."

class CheckerDealsExtremeError(DealsAnalyzerManager):
    gen_message = "Error Occurred While Checking Extreme Deal...."
class CheckerDealsLowestMonthError(DealsAnalyzerManager):
    gen_message = "Error Occurred When Analyze Lowest Monthly Price...."


# ---------- Notifys Exceptions ----------
class NotifyFlightsError(CronosFlightsExceptions):
    gen_message = "General Error Related To NotifyFlights..."


# ---------- Exporter Flights Data Exceptions ----------
class ExporterFlightsDataError(CronosFlightsExceptions):
    gen_message = "General Error Related To ExporterFlightsData..."

class RepositoryNotFoundError(DealsAnalyzerManager):
    gen_message = "Error Repository Could Not Be Found...."
class UploadToGithubError(DealsAnalyzerManager):
    gen_message = "Error Occurred While Trying Upload To GitHub...."
class SaveDataToParquetError(DealsAnalyzerManager):
    gen_message = "Error Occurred While Converting Dataframe To Parquet And Trying To Save...."

