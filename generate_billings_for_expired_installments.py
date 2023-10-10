import asyncio
import logging
import time
from dataclasses import dataclass

from sqlalchemy import select, and_, not_
from sqlalchemy.orm import joinedload

from src.clients.acl.acl_create_billet import CreateBilletClient
from src.clients.acl.adapters.build_payload_for_create_billet import Payload
from src.common.repositories.sqlalchemy_repository import SqlAlchemyRepository
from src.common.service_base import ServiceBase, try_query_except
from src.constants import MASTER, PRE_FIXADO, SLAVE, POS_FIXADO, FINANCING_STATUS, INSTALLMENTS_STATUS, SECURITIZATION
from src.domain.bank_billet.adapter import parse_response_billet, parse_to_payment, parse_to_bank_billet
from src.domain.bank_billet.data.models.bank_billet import BankBillet
from src.domain.bank_billet.data.models.bank_billet_creation_batch_item import BankBilletCreationBatchItem
from src.domain.bank_billet.data.models.payment import Payment
from src.domain.bank_billet.data.repository import (
    BilletInfoRepository,
    RepositoryBankBilletCreationBatch, RepositoryPayment, RepositoryBankBillet,
)
from src.domain.bank_billet.service import BankBilletBatchService, ServiceBankBillet
from src.domain.financial_installments.data.models.address import Address
from src.domain.financial_installments.data.models.customer import Customer
from src.domain.financial_installments.data.models.customers_address import CustomerAddress
from src.domain.financial_installments.data.models.financial_installment import FinancialInstallment
from src.domain.financial_installments.data.models.financing import Financing
from src.domain.financial_installments.data.models.installment_ipca_updates import InstallmentIpcaUpdate
from src.domain.financial_installments.data.models.ipca_index import IpcaIndex
from src.domain.financial_installments.data.models.kobana_wallet import KobanaWallet
from src.infra.database.orm.settings import get_session

logger = logging.getLogger(__name__)

# need installment_id for generate payments

class RepositoryFinancialInstallmentLocal(SqlAlchemyRepository):
    def __init__(self, session):
        super().__init__(session)
        self.entity_model = FinancialInstallment

        self.address = Address
        self.customer_address = CustomerAddress
        self.kobana_wallet = KobanaWallet
        self.bank_billet = BankBillet
        self.customer = Customer

        self.financial_installment = FinancialInstallment
        self.financing = Financing
        self.payment = Payment
        self.batch_item = BankBilletCreationBatchItem
        self.installment_ipca_update = InstallmentIpcaUpdate
        self.ipca_index = IpcaIndex

        self.installment_status = INSTALLMENTS_STATUS
        self.financing_status = FINANCING_STATUS
        self.securitization = SECURITIZATION

    async def find_installments_by_installments_ids_list(self, financing_cet: list[str], installment_id_list: list[int]) \
            -> list[FinancialInstallment]:
        subquery_payments = (
            select(1)
            .where(
                and_(
                    self.payment.financial_installment_id == self.financial_installment.id,
                    self.payment.status != 'canceled',
                    self.payment.type == 'regular',
                )
            )
            .correlate(self.financial_installment)
            .exists()
        )

        stmt = (
            (select(self.financial_installment)
            .join(self.financing, self.financing.id == self.financial_installment.financing_id)
            .where(
                and_(
                    self.financing.cet.in_(financing_cet),
                    self.financing.status.in_(self.financing_status),
                    self.financial_installment.id.in_(installment_id_list),
                    self.financial_installment.status.in_(self.installment_status),
                    self.financial_installment.securitization.in_(self.securitization),
                    not_(subquery_payments),
                )
            )

            )
            .options(
                joinedload(self.financial_installment.financing).options(
                    joinedload(self.financing.customer).options(
                        joinedload(self.customer.customer_address).subqueryload(self.customer_address.address),
                        joinedload(self.customer.customer_provider),
                    ),
                    joinedload(self.financing.financing_providers),
                )
            )
            .options(joinedload(self.financial_installment.kobana_wallets))
        )

        result = await self.session_db.execute(stmt)
        installments = result.scalars().unique().all()

        return installments


@dataclass
class InstallmentService(ServiceBase):
    repository_base: RepositoryFinancialInstallmentLocal

    @try_query_except
    async def get_installments_not_billet(self, financing_cet: list[str],
                                          installment_id_list: list[int]) -> list[FinancialInstallment] | None:
        installments = await self.repository_base.find_installments_by_installments_ids_list(financing_cet,
                                                                                             installment_id_list)

        return installments


async def generate_billets() -> None:
    start_timer = time.time()
    installments_error = []

    financing_cet = [PRE_FIXADO, POS_FIXADO]

    # list of installments to generate payments (billets)
    installments_ids_list = [
        3848141
    ]

    async with get_session(SLAVE) as session:
        repository_installment = RepositoryFinancialInstallmentLocal(session=session)
        installments = await InstallmentService(repository_base=repository_installment).get_installments_not_billet(
            financing_cet,
            installments_ids_list)

    async with get_session(MASTER) as session:
        repository_batch = RepositoryBankBilletCreationBatch(session=session)
        batch = await BankBilletBatchService(repository=repository_batch).create_bank_billet_batch()
        await session.commit()

    logger.info(f'[*] {len(installments)} installments for batch {batch.batch_identifier}')  # noqa G004

    for installment in installments:
        payload = await Payload(
            financial_installments=installment,
            kobana_wallets=installment.kobana_wallets,
            financings=installment.financing,
            customers=installment.financing.customer,
            customer_addresses=installment.financing.customer.customer_address,
            addresses=installment.financing.customer.customer_address.address if installment.financing.customer.customer_address else None,
        ).build_payload_for_create_billet()

        # new expire datatime for a payment (billet)
        payload['expire_at'] = '2023-10-17'

        response = await CreateBilletClient().create_billet(payload)

        await BilletInfoRepository(
            content=response.get('content'),
            status_code=response.get('status_code'),
            batch_id=batch.id,
            installment_id=installment.id,
        ).handle_save_batch_items()

        async with get_session(MASTER) as session:
            repository_billet_batch = RepositoryBankBilletCreationBatch(session=session)
            repository_payment = RepositoryPayment(session=session)
            repository_bank_billet = RepositoryBankBillet(session=session)

            match response.get('status_code'):
                case 201:
                    response_billet = parse_response_billet(response)
                    response_payment = parse_to_payment(
                        installment=installment, provider=response_billet.provider, response_provider=response_billet.provider_response
                    )

                    payment = await ServiceBankBillet(repository_payment=repository_payment).save_payment(response_payment)
                    if payment:
                        bank_billet = parse_to_bank_billet(payment, response_billet.provider_response)
                        await ServiceBankBillet(repository_bank_billet=repository_bank_billet).save_bank_billet(bank_billet)
                case _:
                    installments_error.append(installment.id)
                    await BankBilletBatchService(repository=repository_billet_batch).update_bank_billet_batch(batch.id, 'failed')

    end_timer = time.time()
    response_time_in_minutes = (end_timer - start_timer) / 60

    logger.info(
        f'[*] Process batch  {batch.batch_identifier} with {len(installments)} installments in '  # noqa G004
        f'{response_time_in_minutes:.2f} minutes'
    )


if __name__ == '__main__':
    asyncio.run(generate_billets())
