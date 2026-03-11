from pathlib import Path

from grpc.aio import ServicerContext

from contracts.services.accounts.accounts_service_pb2_grpc import AccountsServiceServicer
from contracts.services.accounts.rpc_get_account_pb2 import GetAccountRequest, GetAccountResponse
from contracts.services.accounts.rpc_get_accounts_pb2 import GetAccountsRequest, GetAccountsResponse
from tests.mock.grpc.tools import get_scenario_grpc
from tests.tools.logger import get_test_logger
from tests.tools.mock import MockLoader

loader = MockLoader(
    root=Path("./tests/mock/grpc/data/accounts"),
    logger=get_test_logger("ACCOUNTS_SERVICE_MOCK_LOADER")
)


class AccountsMockService(AccountsServiceServicer):
    async def GetAccount(self, request: GetAccountRequest, context: ServicerContext) -> GetAccountResponse:
        scenario = await get_scenario_grpc(context)

        return await loader.load_grpc(
            file=f"GetAccount/{scenario}.json",
            model=GetAccountResponse
        )

    async def GetAccounts(self, request: GetAccountsRequest, context: ServicerContext) -> GetAccountsResponse:
        scenario = await get_scenario_grpc(context)

        return await loader.load_grpc(
            file=f"GetAccounts/{scenario}.json",
            model=GetAccountsResponse
        )
