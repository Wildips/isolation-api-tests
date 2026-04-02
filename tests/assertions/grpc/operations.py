import allure

from contracts.services.operations.operation_pb2 import Operation, OperationType, OperationStatus
from contracts.services.operations.rpc_get_operations_pb2 import GetOperationsResponse
from tests.assertions.base import assert_equal
from tests.clients.postgres.operations.model import OperationsTestModel
from tests.schema.operations import OperationEventTestSchema
from tests.tools.date import to_proto_test_datetime
from tests.tools.logger import get_test_logger
from tests.types.operations import OperationTestType, OperationTestStatus

logger = get_test_logger("OPERATIONS_ASSERTIONS")

OPERATION_TYPE_TO_PROTO: dict[OperationTestType, int] = {
    OperationTestType.UNSPECIFIED: OperationType.OPERATION_TYPE_UNSPECIFIED,
    OperationTestType.FEE: OperationType.OPERATION_TYPE_FEE,
    OperationTestType.TOP_UP: OperationType.OPERATION_TYPE_TOP_UP,
    OperationTestType.PURCHASE: OperationType.OPERATION_TYPE_PURCHASE,
    OperationTestType.CASHBACK: OperationType.OPERATION_TYPE_CASHBACK,
    OperationTestType.TRANSFER: OperationType.OPERATION_TYPE_TRANSFER,
    OperationTestType.REVERSAL: OperationType.OPERATION_TYPE_REVERSAL,
    OperationTestType.BILL_PAYMENT: OperationType.OPERATION_TYPE_BILL_PAYMENT,
    OperationTestType.CASH_WITHDRAWAL: OperationType.OPERATION_TYPE_CASH_WITHDRAWAL,
}

OPERATION_STATUS_TO_PROTO: dict[OperationTestStatus, int] = {
    OperationTestStatus.UNSPECIFIED: OperationStatus.OPERATION_STATUS_UNSPECIFIED,
    OperationTestStatus.IN_PROGRESS: OperationStatus.OPERATION_STATUS_IN_PROGRESS,
    OperationTestStatus.COMPLETED: OperationStatus.OPERATION_STATUS_COMPLETED,
    OperationTestStatus.REVERSED: OperationStatus.OPERATION_STATUS_REVERSED,
    OperationTestStatus.FAILED: OperationStatus.OPERATION_STATUS_FAILED,
}


def map_operation_type_to_proto(value: str | OperationTestType) -> int:
    return OPERATION_TYPE_TO_PROTO[OperationTestType(value)]


def map_operation_status_to_proto(value: str | OperationTestStatus) -> int:
    return OPERATION_STATUS_TO_PROTO[OperationTestStatus(value)]


@allure.step("Check operation from event")
def assert_operation_from_event(
        actual: Operation,
        expected: OperationEventTestSchema
) -> None:
    logger.info("Check operation from event")

    assert_equal(actual.type, map_operation_type_to_proto(expected.type), "type")
    assert_equal(actual.status, map_operation_status_to_proto(expected.status), "status")
    assert_equal(actual.amount, expected.amount, "amount")
    assert_equal(actual.user_id, expected.user_id, "user_id")
    assert_equal(actual.card_id, expected.card_id, "card_id")
    assert_equal(actual.category, expected.category, "category")
    assert_equal(actual.created_at, to_proto_test_datetime(expected.created_at), "created_at")
    assert_equal(actual.account_id, expected.account_id, "account_id")


@allure.step("Check operation from model")
def assert_operation_from_model(
        actual: Operation,
        expected: OperationsTestModel
) -> None:
    logger.info("Check operation from model")

    assert_equal(actual.id, expected.id, "id")
    assert_equal(actual.type, map_operation_type_to_proto(expected.type), "type")
    assert_equal(actual.status, map_operation_status_to_proto(expected.status), "status")
    assert_equal(actual.amount, expected.amount, "amount")
    assert_equal(actual.user_id, expected.user_id, "user_id")
    assert_equal(actual.card_id, expected.card_id, "card_id")
    assert_equal(actual.category, expected.category, "category")
    assert_equal(actual.created_at, to_proto_test_datetime(expected.created_at), "created_at")
    assert_equal(actual.account_id, expected.account_id, "account_id")


@allure.step("Check get operations response from events")
def assert_get_operations_response_from_events(
        actual: GetOperationsResponse,
        expected: list[OperationEventTestSchema]
) -> None:
    logger.info("Check get operations response from events")

    assert_equal(len(actual.operations), len(expected), "operations count")
    for index, event in enumerate(expected):
        assert_operation_from_event(actual.operations[index], event)


@allure.step("Check get operations response from models")
def assert_get_operations_response_from_models(
        actual: GetOperationsResponse,
        expected: list[OperationsTestModel]
) -> None:
    logger.info("Check get operations response from models")

    assert_equal(len(actual.operations), len(expected), "operations count")
    for index, model in enumerate(expected):
        assert_operation_from_model(actual.operations[index], model)
